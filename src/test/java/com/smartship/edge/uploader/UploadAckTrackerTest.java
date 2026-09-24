package com.smartship.edge.uploader;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Application ACK 跟踪器单测：watermark、乱序、超时补发、滑动窗口。
 * 时间全部由注入时钟驱动，不靠 sleep。
 */
class UploadAckTrackerTest {

    private static final String MMSI = "413999999";
    private static final String TABLE = "zncb_gps_data";

    @Test
    @DisplayName("T4: 乱序 ACK 只记账，游标停在连续水位，补齐后一次推进")
    void outOfOrderAckAdvancesContiguousWatermarkOnly() {
        UploadAckTracker tracker = new UploadAckTracker();
        tracker.track(MMSI, TABLE, 1001L, "msg-1001");
        tracker.track(MMSI, TABLE, 1002L, "msg-1002");
        tracker.track(MMSI, TABLE, 1003L, "msg-1003");
        tracker.track(MMSI, TABLE, 1004L, "msg-1004");

        assertTrue(tracker.onAck(MMSI, "msg-1001", "1001"));
        assertTrue(tracker.onAck(MMSI, "msg-1002", "1002"));
        assertTrue(tracker.onAck(MMSI, "msg-1004", "1004"));

        assertEquals(1002L, tracker.watermark(MMSI, TABLE, 1000L),
                "1003 缺口前停住，1004 的 ACK 只记账不推进");

        assertTrue(tracker.onAck(MMSI, "msg-1003", "1003"));
        assertEquals(1004L, tracker.watermark(MMSI, TABLE, 1000L),
                "缺口补齐后连续推进到 1004");
    }

    @Test
    @DisplayName("未知 msg_id 与 seq 错位的 ACK 直接忽略")
    void unknownAndMismatchedAckIgnored() {
        UploadAckTracker tracker = new UploadAckTracker();
        tracker.track(MMSI, TABLE, 1001L, "msg-1001");

        assertFalse(tracker.onAck(MMSI, "msg-unknown", null));
        assertFalse(tracker.onAck(MMSI, "msg-1001", "9999"),
                "seq 错位必须忽略，防止错位推进");
        assertEquals(1000L, tracker.watermark(MMSI, TABLE, 1000L));

        assertTrue(tracker.onAck(MMSI, "msg-1001", null),
                "无 seq 的 ACK 按 msg_id 匹配");
        assertEquals(1001L, tracker.watermark(MMSI, TABLE, 1000L));
    }

    @Test
    @DisplayName("超时未 ACK 进入补发集合，重发登记后不再到期")
    void timeoutEntersResendSet() {
        AtomicLong clock = new AtomicLong(1_000_000L);
        UploadAckTracker tracker = new UploadAckTracker(clock::get);
        tracker.track(MMSI, TABLE, 1001L, "msg-1001");

        assertTrue(tracker.resendDueAt(MMSI, TABLE, 30_000L, 1_000_000L + 29_999L).isEmpty());
        assertEquals(1, tracker.resendDueAt(MMSI, TABLE, 30_000L, 1_000_000L + 30_000L).size());

        // 补发后重新登记：时间戳刷新，本轮不再到期。
        tracker.trackAt(MMSI, TABLE, 1001L, "msg-1001", 1_000_000L + 30_000L);
        assertTrue(tracker.resendDueAt(MMSI, TABLE, 30_000L, 1_000_000L + 30_001L).isEmpty());
    }

    @Test
    @DisplayName("在途计数供滑动窗口限流；watermark 推进后裁剪已确认前缀")
    void inFlightCountAndPruning() {
        UploadAckTracker tracker = new UploadAckTracker();
        tracker.track(MMSI, TABLE, 1001L, "msg-1001");
        tracker.track(MMSI, TABLE, 1002L, "msg-1002");
        assertEquals(2, tracker.inFlightCount(MMSI, TABLE));

        tracker.onAck(MMSI, "msg-1001", "1001");
        assertEquals(1, tracker.inFlightCount(MMSI, TABLE));

        assertEquals(1001L, tracker.watermark(MMSI, TABLE, 1000L));
        assertEquals(1, tracker.trackedCount(MMSI, TABLE),
                "已确认前缀被裁剪，内存不随时间增长");
        assertFalse(tracker.onAck(MMSI, "msg-1001", "1001"),
                "裁剪后的迟到重复 ACK 直接忽略");
    }
}
