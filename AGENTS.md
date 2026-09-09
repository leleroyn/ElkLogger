# AGENTS.md — ElkLogger 项目说明

> 供 AI 助手 / 开发者快速上手的上下文记录。

## 项目概览
- **名称**：ElkLogger（`com.jiangtunzj:elk`，模块 `elk-logger`），版本 **2.1.0**
- **作用**：轻量日志组件，异步将日志以 JSON 投递到 RabbitMQ 队列 `ELK-LOGS`（供 ELK 采集）。
- **仓库**：`git@github.com:leleroyn/ElkLogger.git`（remote `origin`，分支 `main`）。

## 目录结构
```
pom.xml                         # 父 pom（聚合 elk-logger）
elk-logger/src/main/java/com/jiangtunzj/utils/log/
  ElkLogger.java                # 核心：init() + log()/logf()，线程池 + RabbitMQ 发送
  LogLevel.java                 # 枚举：DEBUG/INFO/TRACE/WARN/ERROR（getValue() 小写串）
```

## 技术栈与依赖
- **Java 8**，编码 UTF-8。
- 依赖：`lombok`、`com.rabbitmq:amqp-client`（optional）、`com.alibaba:fastjson`。
- `@Slf4j` 所需 `slf4j-api` 经 `amqp-client` 传递引入；**不要**显式引入 slf4j 组件。

## 构建（本机无全局 JAVA_HOME，需显式设置）
- JDK 8（Semeru）：`D:\Users\Administrator\.jdks\semeru-1.8.0_482`
- Maven（IDEA 自带）：`D:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\plugins\maven-plugin\lib\maven3\bin\mvn.cmd`
- 镜像 settings：`D:\Users\leler\.m2\settings.xml`
- 编译单模块：`mvn -s <settings> -pl elk-logger compile`

## 日志 API 约定
- 原有 `log(...)` 系列签名与语义**保持不变**（含 `log(level,title,message,traceId)` 的 traceId 语义）。
- **占位符日志用 `logf(...)`**（f = format），支持 SLF4J 风格 `{}`：
  - `logf(level, message, args...)`
  - `logf(level, title, message, args...)`
  - `logf(level, title, message, traceId, args...)`
- **为什么用 `logf` 而非重载 `log`**：`log(level,String,String,Object...)` 与既有 `log(level,String,String,String)` 传 `null`/`Object` 时会产生重载二义性编译错误；独立方法名对既有调用零破坏。
- `formatMessage` 语义（自实现，无外部依赖）：`{}` 按序填充，多余占位符原样保留、多余参数忽略、`null`→`"null"`；`\{}` 转义为字面量 `{}`；末尾 `Throwable` 未被占位符消费时自动追加堆栈。

## Git 约定
- 提交信息用中文（如 `feat: ...`、`update to 2.0.0`）。
- 未明确要求不要自动 commit/push。
