package com.smartship.edge.collect.modbus.tcp;

/**
 * Modbus 协议级异常
 */
public class ModbusProtocolException extends RuntimeException {

    public ModbusProtocolException(String message) {
        super(message);
    }

    public ModbusProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
