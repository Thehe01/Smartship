package com.smartship.edge.routing.exception;

/**
 * 船舶数据库注册表查询异常
 * <p>
 * 用于区分“船舶不存在”与“主认证数据库查询异常（如连接超时、网络中断等）”，
 * 避免因主库瞬态异常而误判为船舶不存在并误删正常工作的分船连接池。
 */
public class RegistryQueryException extends RuntimeException {

    public RegistryQueryException(String message) {
        super(message);
    }

    public RegistryQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
