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
import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ElkLogger {
    private static final ConnectionFactory rabbitMqFactory = new ConnectionFactory();
    private static String AppName;
    private static String SourceHost;
    private static final String LOG_QUEUE_NAME = "ELK-LOGS";
    private static boolean HasInit = false;

    public static void init(String app, String sourceHost, RabbitMQProperty property) {
        if (!HasInit) {
            ElkLogger.AppName = app;
            ElkLogger.SourceHost = sourceHost;
            rabbitMqFactory.setHost(property.getHost());
            rabbitMqFactory.setUsername(property.getUserName());
            rabbitMqFactory.setPassword(property.getPassword());
            rabbitMqFactory.setPort(property.getPort());
            rabbitMqFactory.setVirtualHost("/");
            HasInit = true;
        }
    }

    public static void init(String app, RabbitMQProperty property) {
        init(app, "", property);
    }

    @SneakyThrows
    public static void log(LogLevel logLevel, String title, String message, String traceId) {
        Holder.taskExecutor.execute(() -> {
            Connection connection = null;
            Channel channel = null;
            try {
                connection = rabbitMqFactory.newConnection();
                channel = connection.createChannel();
                channel.queueDeclare(LOG_QUEUE_NAME, false, false, false, null);
                JSONObject logBody = new JSONObject();
                logBody.put("app_name", AppName);
                if (SourceHost != null && SourceHost.length() > 0) {
                    logBody.put("source_host", SourceHost);
                }
                logBody.put("log_time", new Date());
                logBody.put("log_level", logLevel.getValue());
                if (title != null && title.length() > 0) {
                    logBody.put("log_title", title);
                }
                logBody.put("log_message", message);
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
                    if (connection != null)
                        connection.close();
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

        static {
            BlockingDeque blockingDeque = new LinkedBlockingDeque(200);
            taskExecutor = new ThreadPoolExecutor(2, 4, 300, TimeUnit.SECONDS, blockingDeque);
        }
    }
}
