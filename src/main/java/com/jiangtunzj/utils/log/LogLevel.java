package com.jiangtunzj.utils.log;

public enum LogLevel {
    DEBUG("debug"), INFO("info"), TRACE("trace"), WARN("warn"), ERROR("error");
    private final String value;

    LogLevel(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}