package com.smartship.edge.collect.nmea.service;

import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * NMEA 数据处理器
 * <p>
 * 核心架构特性：
 * 1. 内存滑动时间窗口（CACHE_WINDOW_MS = 2000）合并机制：
 *    将离散的 RMC（速度航向经纬度）与 GGA（海拔卫星数HDOP）在内存中加锁聚合成完整的 GpsSnapshot 快照后落库
 * 2. 航海经纬度格式 (ddmm.mmmm) 到十进制度数的精准数学转换
 * 3. 分段对象锁保护各传感器独立缓存，将锁争用范围控制在纳秒级
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NmeaDataHandler {

    private final NmeaDataPersistenceService persistence;
    private final PersistenceThrottle throttle;

    private final Object gpsLock = new Object();
    private final Object windLock = new Object();
    private final Object depthLock = new Object();

    private final GpsCache gpsCache = new GpsCache();
    private final WindCache windCache = new WindCache();
    private final DepthCache depthCache = new DepthCache();

    private final AtomicLong lastGpsFlush = new AtomicLong(0);
    private final AtomicLong lastWindFlush = new AtomicLong(0);
    private final AtomicLong lastDepthFlush = new AtomicLong(0);

    private static final long CACHE_WINDOW_MS = 2000;

    // ==================== RMC 导航基础帧 ====================
    public void handleRMC(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 10) return;
        try {
            double lat = parseNmeaCoord(f[3], f[4], false);
            double lon = parseNmeaCoord(f[5], f[6], true);
            double speed = parseDouble(f[7]);
            double course = parseDouble(f[8]);
            synchronized (gpsLock) {
                gpsCache.lat = lat;
                gpsCache.lon = lon;
                gpsCache.speed = speed;
                gpsCache.course = course;
                gpsCache.status = f[2]; // A-有效, V-警告
                gpsCache.mmsi = mmsi;
            }
            tryFlushGps(source);
        } catch (Exception e) {
            log.warn("[RMC] 解析异常: {}", e.getMessage());
        }
    }

    // ==================== GGA 卫星定位精度帧 ====================
    public void handleGGA(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 10) return;
        try {
            double lat = parseNmeaCoord(f[2], f[3], false);
            double lon = parseNmeaCoord(f[4], f[5], true);
            int quality = parseInt(f[6]);
            int satellites = parseInt(f[7]);
            double hdop = parseDouble(f[8]);
            double altitude = parseDouble(f[9]);
            synchronized (gpsLock) {
                gpsCache.lat = lat;
                gpsCache.lon = lon;
                gpsCache.altitude = altitude;
                gpsCache.satellites = satellites;
                gpsCache.hdop = hdop;
                gpsCache.quality = quality;
                gpsCache.mmsi = mmsi;
            }
            tryFlushGps(source);
        } catch (Exception e) {
            log.warn("[GGA] 解析异常: {}", e.getMessage());
        }
    }

    // ==================== VTG 航向航速帧 ====================
    public void handleVTG(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 8) return;
        try {
            double course = parseDouble(f[1]);
            double speed = parseDouble(f[5]);
            synchronized (gpsLock) {
                gpsCache.course = course;
                gpsCache.speed = speed;
                gpsCache.mmsi = mmsi;
            }
            tryFlushGps(source);
        } catch (Exception e) {
            log.warn("[VTG] 解析异常: {}", e.getMessage());
        }
    }

    // ==================== HDT 真航向 ====================
    public void handleHDT(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 2) return;
        try {
            double heading = parseDouble(removeChecksum(f[1]));
            synchronized (gpsLock) {
                gpsCache.headingTrue = heading;
                gpsCache.mmsi = mmsi;
            }
            tryFlushGps(source);
        } catch (Exception e) {
            log.warn("[HDT] 解析异常: {}", e.getMessage());
        }
    }

    // ==================== HDG 磁航向 ====================
    public void handleHDG(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 2) return;
        try {
            double headingMag = parseDouble(f[1]);
            double magVar = f.length > 4 ? parseDouble(f[4]) : 0;
            String varDir = f.length > 5 ? removeChecksum(f[5]) : "";
            if ("W".equals(varDir)) magVar = -magVar;
            synchronized (gpsLock) {
                gpsCache.headingMag = headingMag;
                gpsCache.magVar = magVar;
                gpsCache.mmsi = mmsi;
            }
            tryFlushGps(source);
        } catch (Exception e) {
            log.warn("[HDG] 解析异常: {}", e.getMessage());
        }
    }

    private void tryFlushGps(String source) {
        GpsSnapshot snapshot = null;
        synchronized (gpsLock) {
            if (gpsCache.mmsi == null) return;
            long now = System.currentTimeMillis();
            long last = lastGpsFlush.get();
            // 达到 2000ms 时间滑动窗口阈值且包含有效经纬度或航向
            if (now - last > CACHE_WINDOW_MS && (gpsCache.lat != null || gpsCache.headingTrue != null)) {
                snapshot = new GpsSnapshot(
                        gpsCache.lat, gpsCache.lon, gpsCache.speed, gpsCache.course,
                        gpsCache.headingTrue, gpsCache.headingMag, gpsCache.magVar,
                        gpsCache.altitude, gpsCache.satellites, gpsCache.hdop,
                        gpsCache.quality, gpsCache.status, gpsCache.mmsi
                );
                gpsCache.clear();
                lastGpsFlush.set(now);
            }
        }
        if (snapshot != null) {
            // [模块三] CAS 写入节流保护，异步写入分船库
            if (throttle.shouldWrite(snapshot.mmsi() + ":nmea:gps")) {
                persistence.saveGps("FRAME", source,
                        snapshot.lat(), snapshot.lon(), snapshot.speed(), snapshot.course(),
                        snapshot.headingTrue(), snapshot.headingMag(), snapshot.magVar(),
                        snapshot.altitude(), snapshot.satellites(), snapshot.hdop(),
                        snapshot.quality(), snapshot.status(), snapshot.mmsi());
            }
        }
    }

    // ==================== MWV / MWD 风速风向 ====================
    public void handleMWV(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 5) return;
        try {
            double angle = parseDouble(f[1]);
            double speed = parseDouble(f[3]);
            String ref = f[2]; // R-相对, T-理论/真
            synchronized (windLock) {
                if ("R".equalsIgnoreCase(ref)) {
                    windCache.apparentAngle = angle;
                    windCache.apparentSpeed = speed;
                } else {
                    windCache.trueAngle = angle;
                    windCache.trueSpeed = speed;
                }
                windCache.mmsi = mmsi;
            }
            tryFlushWind(source);
        } catch (Exception e) {
            log.warn("[MWV] 解析异常: {}", e.getMessage());
        }
    }

    public void handleMWD(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 8) return;
        try {
            double trueDir = parseDouble(f[1]);
            double speedKnots = parseDouble(f[7]);
            synchronized (windLock) {
                windCache.trueDirection = trueDir;
                windCache.trueSpeed = speedKnots;
                windCache.mmsi = mmsi;
            }
            tryFlushWind(source);
        } catch (Exception e) {
            log.warn("[MWD] 解析异常: {}", e.getMessage());
        }
    }

    private void tryFlushWind(String source) {
        WindSnapshot snapshot = null;
        synchronized (windLock) {
            if (windCache.mmsi == null) return;
            long now = System.currentTimeMillis();
            if (now - lastWindFlush.get() > CACHE_WINDOW_MS) {
                snapshot = new WindSnapshot(
                        windCache.apparentAngle, windCache.apparentSpeed,
                        windCache.trueAngle, windCache.trueDirection, windCache.trueSpeed,
                        windCache.mmsi
                );
                windCache.clear();
                lastWindFlush.set(now);
            }
        }
        if (snapshot != null && throttle.shouldWrite(snapshot.mmsi() + ":nmea:wind")) {
            persistence.saveWind("MWV", source,
                    snapshot.apparentAngle(), snapshot.apparentSpeed(),
                    snapshot.trueAngle(), snapshot.trueDirection(), snapshot.trueSpeed(),
                    snapshot.mmsi());
        }
    }

    // ==================== DBT / DPT 水深 ====================
    public void handleDBT(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 5) return;
        try {
            double depthM = parseDouble(f[3]);
            synchronized (depthLock) {
                depthCache.depthM = depthM;
                depthCache.mmsi = mmsi;
            }
            tryFlushDepth(source);
        } catch (Exception e) {
            log.warn("[DBT] 解析异常: {}", e.getMessage());
        }
    }

    public void handleDPT(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 3) return;
        try {
            double depthM = parseDouble(f[1]);
            double offset = parseDouble(f[2]);
            synchronized (depthLock) {
                depthCache.depthM = depthM;
                depthCache.offsetM = offset;
                depthCache.mmsi = mmsi;
            }
            tryFlushDepth(source);
        } catch (Exception e) {
            log.warn("[DPT] 解析异常: {}", e.getMessage());
        }
    }

    private void tryFlushDepth(String source) {
        DepthSnapshot snapshot = null;
        synchronized (depthLock) {
            if (depthCache.mmsi == null) return;
            long now = System.currentTimeMillis();
            if (depthCache.depthM != null && now - lastDepthFlush.get() > CACHE_WINDOW_MS) {
                snapshot = new DepthSnapshot(depthCache.depthM, depthCache.offsetM, depthCache.mmsi);
                depthCache.clear();
                lastDepthFlush.set(now);
            }
        }
        if (snapshot != null && throttle.shouldWrite(snapshot.mmsi() + ":nmea:depth")) {
            persistence.saveDepth("DPT", source, snapshot.depthM(), snapshot.offsetM(), snapshot.mmsi());
        }
    }

    // ==================== RSA 舵角 ====================
    public void handleRSA(String sentence, String source, String mmsi) {
        String[] f = sentence.split(",");
        if (f.length < 3) return;
        try {
            double rudderAngle = parseDouble(f[1]);
            if (throttle.shouldWrite(mmsi + ":nmea:rudder")) {
                persistence.saveRudder("RSA", source, rudderAngle, mmsi);
            }
        } catch (Exception e) {
            log.warn("[RSA] 解析异常: {}", e.getMessage());
        }
    }

    public void clearCaches() {
        synchronized (gpsLock) {
            gpsCache.clear();
            lastGpsFlush.set(0);
        }
        synchronized (windLock) {
            windCache.clear();
            lastWindFlush.set(0);
        }
        synchronized (depthLock) {
            depthCache.clear();
            lastDepthFlush.set(0);
        }
        log.info("[NmeaDataHandler] 内存聚合缓存已全部清空（切换船舶身份）");
    }

    // ==================== 航海经纬度转换数学工具 ====================
    public static double parseNmeaCoord(String value, String direction, boolean isLong) {
        if (value == null || value.isEmpty()) return 0;
        int dd = isLong ? 3 : 2;
        if (value.length() <= dd) return 0;
        double d = Double.parseDouble(value.substring(0, dd));
        double m = Double.parseDouble(value.substring(dd));
        double c = d + m / 60.0;
        return ("S".equalsIgnoreCase(direction) || "W".equalsIgnoreCase(direction)) ? -c : c;
    }

    private int parseInt(String s) {
        return (s == null || s.isEmpty()) ? 0 : Integer.parseInt(s.trim());
    }

    private double parseDouble(String s) {
        return (s == null || s.isEmpty()) ? 0 : Double.parseDouble(s.trim());
    }

    private String removeChecksum(String s) {
        if (s == null) return "";
        int i = s.indexOf('*');
        return i > 0 ? s.substring(0, i) : s;
    }

    public record GpsSnapshot(Double lat, Double lon, Double speed, Double course,
                              Double headingTrue, Double headingMag, Double magVar,
                              Double altitude, Integer satellites, Double hdop,
                              Integer quality, String status, String mmsi) {}

    public record DepthSnapshot(Double depthM, Double offsetM, String mmsi) {}

    public record WindSnapshot(Double apparentAngle, Double apparentSpeed,
                               Double trueAngle, Double trueDirection, Double trueSpeed,
                               String mmsi) {}

    static class GpsCache {
        Double lat, lon, speed, course, headingTrue, headingMag, magVar, altitude, hdop;
        Integer satellites, quality;
        String status, mmsi;
        void clear() {
            lat = lon = speed = course = headingTrue = headingMag = magVar = altitude = hdop = null;
            satellites = quality = null; status = null; mmsi = null;
        }
    }

    static class WindCache {
        Double apparentAngle, apparentSpeed, trueAngle, trueDirection, trueSpeed;
        String mmsi;
        void clear() {
            apparentAngle = apparentSpeed = trueAngle = trueDirection = trueSpeed = null;
            mmsi = null;
        }
    }

    static class DepthCache {
        Double depthM, offsetM;
        String mmsi;
        void clear() {
            depthM = offsetM = null;
            mmsi = null;
        }
    }
}
