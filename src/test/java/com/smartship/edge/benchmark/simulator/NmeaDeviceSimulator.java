package com.smartship.edge.benchmark.simulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Consumer;

/**
 * NMEA 0183 设备模拟器（测试作用域，无真实串口硬件依赖）。
 * <p>
 * 生成 {@code RMC / GGA / VTG / HDT / MWV / DBT / RSA} 合法语句：逐字节 XOR 校验和真实计算，
 * 时间/位置/航速/风向/水深/舵角逐条漂移；按配置比例混入坏校验与噪声报文。
 * 错误报文必须被 {@code NmeaParser.validateChecksum} 拒绝。
 */
public class NmeaDeviceSimulator {

    private static final String[] TYPES = {"RMC", "GGA", "VTG", "HDT", "MWV", "DBT", "RSA"};

    private final NmeaSimulationConfig config;
    private final Random random;

    // 漂移状态
    private double lat = 31.20;
    private double lon = 121.50;
    private double speed = 8.0;
    private double course = 90.0;
    private long timeSeconds = 10 * 3600L;

    public NmeaDeviceSimulator(NmeaSimulationConfig config) {
        this.config = config;
        this.random = new Random(config.seed());
    }

    public record GenerationResult(List<String> sentences, int validCount, int invalidCount) {
    }

    /**
     * 批量生成固定条数语句（直接驱动 Parser 场景，不做 pacing）。
     */
    public GenerationResult generate(int count) {
        List<String> out = new ArrayList<>(count);
        int invalid = 0;
        for (int i = 0; i < count; i++) {
            double r = random.nextDouble();
            String sentence;
            if (r < config.noiseRate()) {
                sentence = noiseSentence();
                invalid++;
            } else {
                sentence = nextValidSentence();
                if (random.nextDouble() < config.invalidChecksumRate()) {
                    sentence = corruptChecksum(sentence);
                    invalid++;
                }
            }
            out.add(sentence);
        }
        return new GenerationResult(out, count - invalid, invalid);
    }

    /**
     * 按速率流式发射（TCP 场景），达到条数或时长上限即停止。
     */
    public GenerationResult emitPaced(Consumer<String> sink, int count) throws InterruptedException {
        List<String> backlog = generate(count).sentences();
        long nanosPerMsg = config.messageRatePerSecond() > 0
                ? 1_000_000_000L / config.messageRatePerSecond() : 0L;
        long deadline = System.nanoTime() + config.duration().toNanos();
        int valid = 0;
        int invalid = 0;
        for (String s : backlog) {
            if (System.nanoTime() > deadline) {
                break;
            }
            long t0 = System.nanoTime();
            sink.accept(s);
            if (isValidSentence(s)) {
                valid++;
            } else {
                invalid++;
            }
            if (nanosPerMsg > 0) {
                long spent = System.nanoTime() - t0;
                long sleepNanos = nanosPerMsg - spent;
                if (sleepNanos > 0) {
                    Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                }
            }
        }
        return new GenerationResult(backlog, valid, invalid);
    }

    private boolean isValidSentence(String s) {
        int star = s.lastIndexOf('*');
        if (star < 0 || star + 2 >= s.length()) {
            return false;
        }
        int calc = 0;
        for (int i = 1; i < star; i++) {
            calc ^= s.charAt(i);
        }
        try {
            return calc == Integer.parseInt(s.substring(star + 1, star + 3), 16);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String nextValidSentence() {
        advance();
        String type = TYPES[random.nextInt(TYPES.length)];
        String body = switch (type) {
            case "RMC" -> String.format(Locale.ROOT,
                    "GPRMC,%s,A,%s,N,%s,E,%.1f,%.1f,190926,,,",
                    time(), latFmt(), lonFmt(), speed, course);
            case "GGA" -> String.format(Locale.ROOT,
                    "GPGGA,%s,%s,N,%s,E,1,%02d,%.1f,%.1f,M,,M,,",
                    time(), latFmt(), lonFmt(),
                    6 + random.nextInt(6), 0.5 + random.nextDouble() * 1.5,
                    8.0 + random.nextDouble() * 4.0);
            case "VTG" -> String.format(Locale.ROOT,
                    "GPVTG,%.1f,T,,M,%.1f,N,,K", course, speed);
            case "HDT" -> String.format(Locale.ROOT,
                    "GPHDT,%.1f,T", course + random.nextGaussian());
            case "MWV" -> String.format(Locale.ROOT,
                    "WIMWV,%.1f,R,%.1f,N,A",
                    random.nextDouble() * 360.0, 5.0 + random.nextDouble() * 20.0);
            case "DBT" -> {
                double m = 10.0 + random.nextDouble() * 30.0;
                yield String.format(Locale.ROOT,
                        "SDDBT,%.1f,f,%.1f,M,%.1f,F", m * 3.28084, m, m * 0.546807);
            }
            case "RSA" -> String.format(Locale.ROOT,
                    "IIRSA,%.1f,A,%.1f,V", (random.nextDouble() - 0.5) * 70.0,
                    (random.nextDouble() - 0.5) * 10.0);
            default -> throw new IllegalStateException(type);
        };
        return withChecksum(body);
    }

    private void advance() {
        timeSeconds++;
        lat += (random.nextDouble() - 0.5) * 0.0004;
        lon += (random.nextDouble() - 0.5) * 0.0004;
        speed = Math.max(0, speed + (random.nextDouble() - 0.5) * 0.6);
        course = (course + (random.nextDouble() - 0.5) * 4.0 + 360.0) % 360.0;
    }

    private String time() {
        long t = timeSeconds % 86400;
        return String.format(Locale.ROOT, "%02d%02d%02d.00", t / 3600, (t / 60) % 60, t % 60);
    }

    private String latFmt() {
        int dd = (int) lat;
        double mm = (lat - dd) * 60.0;
        return String.format(Locale.ROOT, "%02d%07.4f", dd, mm);
    }

    private String lonFmt() {
        int ddd = (int) lon;
        double mm = (lon - ddd) * 60.0;
        return String.format(Locale.ROOT, "%03d%07.4f", ddd, mm);
    }

    private static String withChecksum(String body) {
        int calc = 0;
        for (int i = 0; i < body.length(); i++) {
            calc ^= body.charAt(i);
        }
        return "$" + body + "*" + String.format(Locale.ROOT, "%02X", calc);
    }

    private static String corruptChecksum(String sentence) {
        int star = sentence.lastIndexOf('*');
        if (star < 0) {
            return sentence;
        }
        char last = sentence.charAt(sentence.length() - 1);
        char flipped = (last == '0') ? '1' : '0';
        return sentence.substring(0, sentence.length() - 1) + flipped;
    }

    private String noiseSentence() {
        return switch (random.nextInt(4)) {
            case 0 -> "$GPXXX,garbage-no-checksum";
            case 1 -> "$GPRMC,corrupt";
            case 2 -> "";
            default -> "not-a-sentence-at-all";
        };
    }
}
