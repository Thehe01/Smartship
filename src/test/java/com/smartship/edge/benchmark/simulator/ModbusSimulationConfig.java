package com.smartship.edge.benchmark.simulator;

/**
 * Modbus 模拟配置（概率 0~1；置 1.0 即确定性注入）。
 *
 * @param responseDelayMs                正常响应前延迟（毫秒，timeout 场景可大于客户端 SoTimeout）
 * @param disconnectProbability          收到请求后直接断开连接的概率
 * @param exceptionResponseProbability   返回 0x83 异常响应的概率
 * @param wrongTransactionIdProbability  返回错误 txId 的概率
 * @param malformedFrameProbability      返回截断畸形帧的概率
 * @param seed                           随机种子（固定种子保证可重复）
 */
public record ModbusSimulationConfig(
        long responseDelayMs,
        double disconnectProbability,
        double exceptionResponseProbability,
        double wrongTransactionIdProbability,
        double malformedFrameProbability,
        long seed) {

    public static ModbusSimulationConfig normal() {
        return new ModbusSimulationConfig(0, 0, 0, 0, 0, 42L);
    }

    public static ModbusSimulationConfig faulty(long responseDelayMs,
                                                double disconnectProbability,
                                                double exceptionResponseProbability,
                                                double wrongTransactionIdProbability,
                                                double malformedFrameProbability) {
        return new ModbusSimulationConfig(responseDelayMs, disconnectProbability,
                exceptionResponseProbability, wrongTransactionIdProbability,
                malformedFrameProbability, 42L);
    }
}
