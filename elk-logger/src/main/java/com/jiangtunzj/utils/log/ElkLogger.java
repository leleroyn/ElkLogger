package com.jiangtunzj.utils.log;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ElkLogger {

    private static String AppName;
    private static String SourceHost;
    private static final String LOG_QUEUE_NAME = "ELK-LOGS";
    private static boolean HasInit = false;
    private static final String Omit = "...";
    private static int OmitLength = 10000;
    private static final ReentrantLock reentrantLock = new ReentrantLock();

    public static void init(String app, String sourceHost, RabbitMQProperty property, Integer maxLength) {
        if (!HasInit) {
            try {
                AppName = app;
                SourceHost = sourceHost;
                Holder.rabbitMqFactory.setHost(property.getHost());
                Holder.rabbitMqFactory.setUsername(property.getUserName());
                Holder.rabbitMqFactory.setPassword(property.getPassword());
                Holder.rabbitMqFactory.setPort(property.getPort());
                Holder.rabbitMqFactory.setVirtualHost("/");
                if (maxLength != null) {
                    OmitLength = maxLength;
                }
                log(LogLevel.DEBUG, String.format("The elk-logger component has been initialized and the message's maxlength has been set to %s .", OmitLength));
                log.debug(String.format("The elk-logger component has been initialized and the message's maxlength has been set to %s .", OmitLength));
                HasInit = true;
            } catch (Exception exception) {
                log.error("The elk-logger component has been initialized error state.", exception);
            }
        }
    }

    public static void init(String app, RabbitMQProperty property) {
        init(app, "", property, null);
    }

    public static void init(String app, RabbitMQProperty property, Integer maxLength) {
        init(app, "", property, maxLength);
    }

    public static void init(String app, String sourceHost, RabbitMQProperty property) {
        init(app, sourceHost, property, null);
    }

    @SneakyThrows
    public static void log(LogLevel logLevel, String title, String message, String traceId) {
        Holder.taskExecutor.execute(() -> {
            Channel channel = null;
            try {
                if (Holder.rabbitMqConnection == null || !Holder.rabbitMqConnection.isOpen()) {
                    try {
                        reentrantLock.lock();
                        if (Holder.rabbitMqConnection == null || !Holder.rabbitMqConnection.isOpen()) {
                            Holder.rabbitMqConnection = Holder.rabbitMqFactory.newConnection();
                        }
                    } finally {
                        reentrantLock.unlock();
                    }
                }
                channel = Holder.rabbitMqConnection.createChannel();
                channel.queueDeclare(LOG_QUEUE_NAME, false, false, false, null);
                JSONObject logBody = new JSONObject();
                logBody.put("app_name", AppName);
                if (SourceHost != null && SourceHost.length() > 0) {
                    logBody.put("source_host", SourceHost);
                }
                logBody.put("log_time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                logBody.put("log_level", logLevel.getValue());
                if (title != null && title.length() > 0) {
                    logBody.put("log_title", subString(title, 1000));
                }
                logBody.put("log_message", subString(message, OmitLength));
                if (traceId != null && traceId.length() > 0) {
                    logBody.put("trace_id", traceId);
                }
                channel.basicPublish("", LOG_QUEUE_NAME, null, logBody.toJSONString().getBytes(StandardCharsets.UTF_8));
            } catch (Exception ex) {
                log.error("写elk日志时异常.", ex);
            } finally {
                try {
                    if (channel != null)
                        channel.close();
                } catch (Exception ex) {
                    log.error("释放elk mq 连接时异常.", ex);
                }
            }
        });
    }

    @SneakyThrows
    public static void log(LogLevel logLevel, String title, String message) {
        log(logLevel, title, message, null);
    }

    @SneakyThrows
    public static void log(LogLevel logLevel, String message) {
        log(logLevel, "", message, null);
    }

    @Data
    @Builder
    public static class RabbitMQProperty {
        private String host;
        private int port = 5672;
        private String userName;
        private String password;
    }

    private static final class Holder {
        static final ThreadPoolExecutor taskExecutor;
        static final ConnectionFactory rabbitMqFactory = new ConnectionFactory();
        static Connection rabbitMqConnection;

        static {
            ArrayBlockingQueue blockingDeque = new ArrayBlockingQueue(1000);
            taskExecutor = new ThreadPoolExecutor(2, 4, 300, TimeUnit.SECONDS, blockingDeque);
        }
    }

    private static String subString(String input, int length) {
        if (input != null && input.length() > length) {
            input = input.substring(0, length) + Omit;
        }
        return input;
    }
}
