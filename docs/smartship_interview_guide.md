# SmartShip 智慧船舶边缘数据接入平台｜面试手册

> 更新日期：2026-09-08  
> 核对口径：以当前工作区源码和测试为准。本文只描述已经实现、能够从代码或测试解释的能力，不使用未经压测的性能数字。

## 目录

1. [项目总览与一分钟介绍](#一项目总览与一分钟介绍)
2. [模块一：多协议数据接入与解析](#二模块一多协议数据接入与解析)
3. [模块二：Collector 生命周期与 TCP 帧治理](#三模块二collector-生命周期与-tcp-帧治理)
4. [模块三：分船数据源路由与持久化保护](#四模块三分船数据源路由与持久化保护)
5. [模块四：弱网增量与快照上报](#五模块四弱网增量与快照上报)
6. [测试证据与面试边界](#六测试证据与面试边界)

---

# 一、项目总览与一分钟介绍

## 1.1 项目定位

SmartShip 是船端边缘数据接入平台。系统从串口、TCP、UDP 接入导航和机舱设备数据，解析 NMEA 0183、AIS 与 Modbus TCP 报文，根据 `shipId/MMSI` 路由到对应船舶数据库，再通过 MQTT 将遥测、快照和事件数据上报岸端。

四个适合面试展开的模块如下：

| 模块 | 一句话亮点 | 可核验事实 | 不能夸大的边界 |
|---|---|---|---|
| 多协议接入 | 文本协议与二进制协议统一接入 | 串口/TCP/UDP、NMEA checksum、Modbus MBAP、AIS 6-bit | AIS 没有完整多分片组装 |
| Collector 治理 | 统一 12 类 Socket Collector 的公共生命周期 | 抽象基类、完整帧读取、命名线程、资源关闭 | 有界队列和看门狗并非 12 类全部启用 |
| 分船持久化 | 注册表驱动的独立 MySQL/HikariCP 路由 | 主机/端口/库名校验、连接池替换与关闭、CAS 节流 | 不是自动创建船舶注册记录，也不等于安全意义上的多租户平台 |
| 弱网上报 | 游标/内容指纹记录进度，失败时不越过断点 | 9 路遥测 + 4 路快照 + 3 路事件，共 16 路 | MQTT QoS 1 是至少一次，不是 exactly-once |

## 1.2 一分钟介绍模板

> 我在 SmartShip 中主要处理四类问题。第一，使用串口、TCP、UDP 接入 NMEA 0183、AIS 和 Modbus TCP 数据，对 NMEA checksum、Modbus MBAP 头和响应字段进行校验，并把设备字段转换为业务数据。第二，将 12 类 Socket Collector 的连接、线程和关闭逻辑下沉到抽象基类，同时抽取完整帧读取组件，避免把一次 TCP `read()` 误当成一帧。第三，根据 `shipId/MMSI` 从注册表路由独立的 MySQL/HikariCP 数据源，在异步持久化层集中执行按船舶、数据类型的写入节流。第四，实现 16 路遥测、快照和事件上报，增量表使用 ID 游标，快照表使用逐行内容指纹，MQTT 失败后停止推进进度，并用稳定 `msg_id` 为岸端幂等提供依据。相关模块当前共有 41 项测试。

## 1.3 简历对应表述

> **SmartShip｜智慧船舶边缘数据接入平台**
>
> - 实现串口、TCP、UDP 多协议接入：解析 NMEA 0183、AIS 与 Modbus TCP 报文，校验 NMEA checksum 和 Modbus MBAP，并对 AIS 首分片 6-bit payload 解码提取 MMSI。
> - 重构 12 类 Socket Collector：抽取公共生命周期与帧读取组件，统一连接、超时看门狗、粘包拆包、命名线程及资源释放逻辑。
> - 按 `shipId/MMSI` 路由独立 MySQL/HikariCP 数据源，校验主机、端口与库名并管理连接池生命周期；将写入节流集中到持久化层，按船舶和数据类型控制最小写入间隔。
> - 实现 16 路遥测、快照及事件上报：基于 MySQL 游标与逐行内容指纹记录进度，MQTT QoS 1 失败即停止推进游标，并以稳定 `msg_id` 支持弱网断点恢复与岸端幂等去重；相关模块 41 项测试通过。

---

# 二、模块一：多协议数据接入与解析

## 2.1 当前实现

- 使用 `jSerialComm` 枚举并监听物理串口或 USB 虚拟串口；也可通过 TCP、UDP 接收 NMEA 数据。
- 串口使用行缓冲识别 `\r`、`\n`、`\r\n`，并设置 65536 字节上限，避免异常输入导致缓存无限增长。
- TCP 使用 `BufferedReader.readLine()` 按行接收；UDP 在单个数据报内按换行拆分。三种输入最终进入同一个 `NmeaParser`。
- NMEA 报文带 `*XX` 时执行 XOR checksum 校验；当前为了兼容不带 checksum 的设备，缺少 `*` 的报文会被接受。
- `!AIVDO` 用于识别本船 MMSI。只解析首分片，并支持 Type 1/2/3/18/19 的前 38 bit；后续分片不会被误当成新 AIS 报文。
- 初次发现 MMSI 时立即采用并准备船库；已有动态 MMSI 发生变化时，需要连续 3 帧一致才切换。若启动时已有配置值，则将其锁定，不允许 AIS 覆盖。
- NMEA 导航、风、水深和机舱字段分别在 2 秒窗口内聚合为快照，再异步持久化并发布实时 MQTT 数据。
- Modbus TCP 客户端校验事务 ID、协议 ID、从站 ID、功能码、MBAP 长度和响应字节数；寄存器按大端序还原，再根据从站 ID 分发到主机、发电机或舵机处理。

## 2.2 源码映射

| 环节 | 源码 | 关键点 |
|---|---|---|
| 串口接入 | `SerialNmeaReceiver` | 自动/显式端口选择、事件监听、行缓冲上限、`@PreDestroy` 关闭 |
| TCP/UDP 接入 | `TcpNmeaReceiver`、`UdpNmeaReceiver` | TCP 连接数上限与读超时；UDP 数据报拆行 |
| NMEA 分发 | `NmeaParser` | checksum、Talker/Type 识别、MMSI 状态切换 |
| AIS 解码 | `AisPayloadDecoder` | 6-bit armoring、fill bits、Type 与 MMSI 位域 |
| 数据聚合 | `NmeaDataHandler` | 分类型锁、2 秒窗口、不可变 Snapshot、MMSI 切换清缓存 |
| Modbus TCP | `ModbusTcpClient`、`ModbusTcpServer` | `readFully`、MBAP 长度/协议校验、异常响应处理 |
| 工程量换算 | `ModbusParser`、`ModbusDataHandler` | 大端寄存器、比例缩放、有符号舵角、从站分流 |

## 2.3 高频追问

### Q1：串口和 TCP 都可能拆包、粘包，你怎么处理？

> NMEA 是行协议，因此串口端持续把新字节追加到每个端口独立的 `StringBuilder`，循环查找 `\r` 或 `\n`；完整行送下游，末尾半行留到下一次事件继续拼接。TCP 端由 `BufferedReader.readLine()` 完成同样的行边界处理。为了防止设备持续发送无换行垃圾数据，串口缓冲设置了 65536 字节上限。二进制 Collector 不使用行分隔，而是由 `SocketFrameReader` 按配置长度或 Modbus 功能码读取完整帧。

### Q2：NMEA checksum 是不是强制的？

> 当前不是“所有报文强制带 checksum”。带 `*XX` 的报文会重新计算起始符之后到 `*` 之前字符的 XOR，并丢弃不匹配或格式非法的报文；没有 `*` 的报文为了兼容部分现场设备仍会通过。如果业务要求严格模式，后续应增加配置项，选择是否拒绝无 checksum 报文。

### Q3：AIS 多分片怎么处理？

> MMSI 位于 AIS 消息开头的前 38 bit，因此当前身份识别只解码首分片，明确跳过第二片及后续分片，避免把后续 payload 的开头错误解释为新的消息头。这个实现足够完成本船 MMSI 提取，但没有做序列号、通道、分片数量维度的完整组装，所以不能声称支持完整多分片 AIS 解码。

### Q4：为什么 MMSI 要区分首次发现和后续切换？

> 冷启动时系统没有船舶身份，首个合法 `!AIVDO` 需要立即触发注册表解析、Schema 准备和 MQTT 重连；如果已经存在 MMSI，则单个误码帧不应切换船库，因此变更值必须连续出现 3 次。已有静态配置时直接锁定，不接受动态覆盖。切换成功前还会清空 NMEA 聚合缓存，避免旧船快照混入新船。

### Q5：Modbus TCP 如何防止把半包当完整响应？

> 客户端先循环读取完整 7 字节 MBAP 头，再根据长度字段读取完整 PDU，而不是依赖单次 `read()`。随后校验协议 ID、事务 ID、从站 ID、功能码、总长度和 byte count；任一不匹配都会断开连接，下一轮重新建立连接。这样同时处理了 TCP 分片和串响应错配问题。

## 2.4 面试边界

- 不说“实现了串口底层驱动”，应说“基于 `jSerialComm` 实现串口接入与事件监听”。
- 不说“所有 NMEA 报文都有严格 checksum”，当前兼容无 checksum 输入。
- 不说“AIS 完整协议解析”，当前只支持特定位置报告类型的 MMSI 提取。
- 不说“经纬度、航向都做了完整业务范围校验”，当前主要依赖字段解析和协议校验。
- 不说“绝对不丢包”，串口、UDP、进程崩溃和硬件故障都不具备绝对保证。

---

# 三、模块二：Collector 生命周期与 TCP 帧治理

## 3.1 当前实现

- 12 类 Socket Collector 统一继承 `AbstractSocketCollector`，公共连接、线程创建、Socket 关闭、看门狗和寄存器地址工具集中维护。
- 每个 Collector 默认使用 2 个命名守护线程，格式为 `Collector-{deviceCode}-{deviceName}-{counter}`，便于日志和线程栈定位。
- `SocketFrameReader` 对固定长度帧执行循环读取；对未配置长度的 Modbus RTU 响应，根据功能码与 byte count 推导总长度，并限制单帧不超过 8192 字节。
- 4 类采集器使用容量为 100 的 `LinkedBlockingQueue` 解耦读取与解析：`SocketListenCollector`、`SocketListenFixLengthCollector`、`SocketPollingCollector`、`SocketPollingSingleCollector`。
- 4 类轮询采集器启用应用层看门狗：`SocketPollingCollector`、`SocketPollingMultiCollector`、`SocketPollingMultiV2Collector`、`SocketPollingSingleCollector`。其他 Collector 复用基类生命周期，但不应描述为都启用了看门狗。
- `close()` 先改变运行状态并关闭 Socket，再对调度器和工作线程池调用 `shutdownNow()` 与 `awaitTermination(2s)`。

## 3.2 12 类 Collector

1. `SocketListenCollector`
2. `SocketListenFixLengthCollector`
3. `SocketListenMultiFixCollector`
4. `SocketListenMultiFlexCollector`
5. `SocketListenMultiNoQueryCollector`
6. `SocketListenRegisterJoinCollector`
7. `SocketPollingCollector`
8. `SocketPollingMultiCollector`
9. `SocketPollingMultiMeterCollector`
10. `SocketPollingMultiMeterV2Collector`
11. `SocketPollingMultiV2Collector`
12. `SocketPollingSingleCollector`

不建议在简历中写具体“减少多少行代码”，因为统计结果会随注释、空行和新增公共组件变化。稳定、可核验的成果是：12 类 Collector 的重复生命周期逻辑已集中，帧读取规则有独立组件和测试。

## 3.3 高频追问

### Q1：为什么不能把一次 `InputStream.read()` 当成一帧？

> TCP 只保证有序字节流，不保留应用层报文边界。一次 `read()` 可能只返回半帧，也可能包含多帧数据。固定长度场景下，`SocketFrameReader.readFixedFrame` 循环读取直到填满目标长度；如果连接在半帧时关闭，则抛出带已读长度的 `EOFException`。Modbus RTU 响应则先读取 3 字节前缀，再根据功能码和 byte count 计算剩余长度。

### Q2：看门狗和 `SoTimeout` 有什么区别？

> `SoTimeout` 约束单次阻塞读等待时间；看门狗比较 `lastDataTime` 与业务允许的最大静默时长。达到阈值后主动关闭 Socket，使阻塞读退出，并由采集循环进入后续重连。看门狗是基类提供的能力，但当前只在 4 类轮询 Collector 中启用。

### Q3：为什么队列容量设置为 100？

> 这里的关键不是数字 100 本身，而是“有界”。读取速度短时间高于解析速度时，队列吸收抖动；持续积压达到上限后，`put()` 会阻塞读取线程，将背压传回 Socket，而不是让内存无限增长。容量应通过报文大小、设备频率和可接受延迟压测调整。当前只有 4 类 Collector 使用该队列，不能泛化到全部 12 类。

### Q4：`shutdownNow()` 是否能强制杀死线程？

> 不能。`shutdownNow()` 发送中断并返回尚未执行的任务，线程能否及时退出取决于任务是否响应中断以及底层 I/O 是否被关闭。因此代码先关闭 Socket，再中断线程池，并等待 2 秒；若仍未结束只记录告警。捕获 `InterruptedException` 后恢复中断标志，避免上层取消语义丢失。

### Q5：守护线程是否等于没有线程泄漏？

> 不等于。守护线程只是不阻止 JVM 在没有用户线程时退出，不能代替显式资源回收。真正的治理仍依赖 `running` 状态、Socket 关闭、线程池 shutdown 和任务对中断的响应。命名线程的价值主要是提高 `jstack` 和日志定位效率。

## 3.4 面试边界

- 不说“所有 12 类 Collector 都使用有界队列或看门狗”。
- 不说“守护线程杜绝线程泄漏”。
- 不说“`shutdownNow()` 强制销毁线程”。
- 不使用未经重新统计和固定口径的代码行数降幅。
- 可以强调 `SocketFrameReader` 对半帧、EOF、非法长度和 Modbus 功能码的测试覆盖。

---

# 四、模块三：分船数据源路由与持久化保护

## 4.1 当前实现

- `ShipDataSourceManager` 根据 `shipId` 或 `MMSI` 查询 `ship_database_registry`，拒绝不存在或禁用的注册记录。
- 每条注册记录可配置独立的主机、端口、数据库名、用户名和密码，并创建对应的 HikariCP 与 `JdbcTemplate`。
- 数据库名只允许字母、数字、下划线；主机格式和 1～65535 端口也会校验，避免把未经验证的注册表字段直接拼入 JDBC URL。
- 连接池以 `shipId` 为优先键、MMSI 为备用键；同一船舶的注册表配置变化时，通过 `ConcurrentHashMap.compute` 原子替换连接，并关闭旧 HikariCP。
- 应用退出时统一关闭所有已缓存的连接池。
- `ShipAutoRegisterService` 的实际职责是：确认船已存在于注册表且启用，然后在目标库执行 `serialdata-schema.sql`，设置 `schemaReady` 并持久化 MMSI。它不会自动新增 `ship_database_registry` 记录。
- NMEA 与 Modbus 写入节流只在异步持久化服务中执行，避免处理层和持久化层使用同一 key 重复节流。
- `PersistenceThrottle` 使用 `System.nanoTime()`、`ConcurrentHashMap<String, AtomicLong>` 和 CAS 按业务 key 控制最小写入间隔，并周期清理长期不用的 key。
- `@Async` 使用有界线程池和 `CallerRunsPolicy`：正常情况下解耦采集与 JDBC；队列饱和时由调用线程执行，形成背压，而不是无限堆积任务。

## 4.2 源码映射

| 环节 | 源码 | 关键点 |
|---|---|---|
| 注册表解析与连接池 | `ShipDataSourceManager` | `resolveRegistry`、`validateRegistry`、`connectionKey`、`compute` 替换 |
| Schema 准备 | `ShipAutoRegisterService` | 只处理已注册且启用的船；执行幂等建表脚本 |
| 写入节流 | `PersistenceThrottle` | 单调时钟、CAS、按 key 隔离、陈旧 key 清理 |
| NMEA 持久化 | `NmeaDataPersistenceService` | `@Async`、`schemaReady`、功能开关、统一节流、动态数据源 |
| Modbus 持久化 | `ModbusDataPersistenceService` | 按设备类型和 slaveId 建立节流 key |
| 异步线程池 | `AsyncExecutorConfig` | 有界队列、命名线程、`CallerRunsPolicy`、停机等待 |

## 4.3 高频追问

### Q1：分船数据路由如何工作？

> 业务侧只传 `shipId` 或 MMSI，管理器先在授权库查询启用的注册记录，校验数据库地址信息，再根据连接键从 `ConcurrentHashMap` 获取或创建 `ConnectionHolder`。每个 Holder 包含注册表快照、HikariCP 和 `JdbcTemplate`。如果同一船的连接配置改变，`compute` 会创建新连接并关闭旧连接，避免继续使用过期数据源。

### Q2：这是不是多租户物理隔离？

> 更准确的说法是“注册表驱动的分船独立数据源路由”。不同船可以指向不同数据库甚至不同主机，但当前模块没有提供完整多租户平台所需的租户鉴权、配额、审计和跨租户安全证明，因此不把它包装成严格的多租户隔离方案。

### Q3：为什么用 `System.nanoTime()` 做节流？

> 节流关心的是时间间隔而不是日期。`currentTimeMillis()` 可能因系统校时回拨或跳变，`nanoTime()` 更适合计算单调的相对时间。相同业务 key 使用独立 `AtomicLong`；先走时间差快路径，满足间隔后再 CAS 更新时间，保证同一 key 的并发请求只有一个通过。

### Q4：为什么节流一定放在持久化层？

> 处理层调用的是 `@Async` 代理。如果处理层先用相同 key 消耗一次时间窗口，异步持久化线程再次检查时会立即被拦截，结果是业务日志显示已经解析，但数据库没有写入。现在每条写入路径只在持久化服务执行一次节流，使“是否允许写入”和真正的 JDBC 操作处在同一个职责边界。

### Q5：`@Async` 是否保证采集线程永不阻塞？

> 不保证。配置使用有界队列，队列满时 `CallerRunsPolicy` 会让提交任务的线程自己执行，这是主动背压，可以避免任务无限堆积，但会暂时拖慢上游。面试中应说“正常负载下异步解耦、过载时有界背压”，不能说“完全零阻塞”。

### Q6：新 MMSI 是否会自动创建船舶注册信息？

> 不会。AIS 识别出 MMSI 后，`ensureRegistered` 只查询既有的 `ship_database_registry`。不存在或未启用时会把 `schemaReady` 设为 false 并停止持久化；只有已登记的船才会连接目标数据库、执行幂等建表脚本并进入可写状态。

## 4.4 面试边界

- 不写“节流减少 80% I/O”等未经测量的数据。
- 不说“异步持久化绝不阻塞”，有界线程池会在过载时背压。
- 不说“发现任意新船后零配置自动注册”，注册表必须预先存在并启用。
- 不说“每艘船一定是独立物理服务器”，这是注册表配置能力，不是部署事实。

---

# 五、模块四：弱网增量与快照上报

## 5.1 当前实现

- 默认初始延迟 5 秒、固定延迟 15 秒执行轮询；批大小默认 50，均可配置。
- 使用 `AtomicBoolean` 防止上一轮未结束时发生调度重叠。
- 共 16 路业务数据：
  - 9 路增量遥测：GPS、风、水深、舵角、NMEA 机舱、主机、电气、舵机、电表；
  - 4 路当前快照：AIS 目标、当前告警、当前采集值、设备状态；
  - 3 路增量事件：监控告警、告警记录、高风险告警响应。
- 12 路增量流按 `id > last_uploaded_id ORDER BY id` 查询；逐条发布，失败后 `break`，只把游标推进到最后一条成功记录。
- 4 路快照不能只依赖时间戳。当前按主键做 keyset 分页，对每行规范化 JSON 计算 SHA-256，并将前 64 bit 作为内容指纹，只有内容变化才上报；发布成功后才更新该行指纹。
- 每条 Payload 补充 `ship_id`、`mmsi`、`local_id`、`source_database`、`type`、发送时间与稳定 `msg_id`。
- MQTT 默认 QoS 1、自动重连、内存持久化和 clean session；`ssl://` 地址可启用 TLS Socket。默认配置仍是普通 `tcp://`，因此只能说“具备 TLS 接入分支”，不能说当前部署必然加密。

## 5.2 进度模型

```text
增量流：last_uploaded_id -> 查询后续 ID -> 逐条 publish
                                 ├─ 成功：记录 maxSuccessId
                                 └─ 失败：break，不越过失败行

快照流：partition_key -> 上次内容指纹
          全表按主键 keyset 扫描 -> 当前内容指纹
                                 ├─ 相同：跳过
                                 ├─ 不同且发布成功：更新指纹
                                 └─ 发布失败：停止，不更新指纹
```

`zncb_upload_cursor.last_uploaded_id` 在增量流中保存真实 ID，在快照流的分区记录中保存内容指纹。`last_uploaded_time` 字段当前保留但不参与快照推进。

## 5.3 高频追问

### Q1：为什么 QoS 1 还需要稳定 `msg_id`？

> QoS 1 的语义是至少一次。客户端或网络可能在 Broker 已收到消息、但确认过程异常时重发；进程也可能在 publish 成功后、更新本地游标前退出。稳定 `msg_id` 让同一源记录重发时保持相同标识，岸端可以通过唯一索引或去重缓存实现幂等。它不是边缘端单方面实现的 exactly-once，最终仍依赖岸端按 `msg_id` 去重。

### Q2：第 50 条发送失败，为什么必须停止？

> 若继续发送并把游标推进到更大的 ID，失败的第 50 条将被永久越过。当前循环在 publish 返回 false 时立即 `break`，随后只更新到此前成功的最大 ID。网络恢复后下一轮会重新从失败记录开始。代价是单条持续失败的毒丸数据可能阻塞后续记录，因此后续可增加失败计数、死信记录和人工重放。

### Q3：为什么快照流改为逐行内容指纹？

> `updated_at > lastTime` 容易受数据库秒级时间精度、同秒多次更新和时钟边界影响，而且快照表同一主键会原地覆盖。内容指纹直接判断“这一行的实际内容是否变化”，配合主键 keyset 扫描，不依赖时间戳推进；发布失败时不更新指纹，所以下轮仍会重试。

### Q4：稳定 `msg_id` 如何生成，有什么限制？

> 当前 canonical string 为 `mmsi|type|sourceIdentity|sourceTime`，其中 identity 从 `source_id/external_alarm_id/local_id/id/device_code` 依次选择，时间从 `update_time/updated_at/rec_time/alarm_time/time/timestamp` 依次选择，然后计算 SHA-256。对带唯一 ID 的增量记录能够稳定复现。限制是：若快照同一行在同一时间精度内多次变化，identity 和 sourceTime 可能不变，此时不同内容可能得到相同 `msg_id`。若要进一步加强，可把快照内容指纹或显式版本号纳入 canonical string。

### Q5：这是不是 Transactional Outbox？

> 不是。当前直接扫描业务表，没有在业务写入事务中同步写入 outbox 事件，也没有 `PENDING/SENDING/SENT/DEAD` 状态机和岸端业务 ACK。准确定位是“本地业务表 + 进度表 + MQTT QoS 1 + 稳定消息标识”的轻量弱网恢复方案。若可靠性要求升级，可以演进为事务 Outbox、失败计数/死信和应用层 ACK。

## 5.4 面试边界

- 不说 exactly-once、端到端零丢失或 100% 幂等。
- 不再把快照流描述为 `updated_at` 时间戳游标；当前是逐行内容指纹。
- 不把 Broker 的 PUBACK 等同于岸端业务数据库已落库。
- 不说实现了标准 Transactional Outbox。
- 不把 TLS 能力分支描述为生产环境已经启用 TLS。

---

# 六、测试证据与面试边界

## 6.1 当前测试数量

| 模块 | `@Test` 数量 | 主要覆盖 |
|---|---:|---|
| `smartship-edge/source/collect` | 21 | NMEA checksum/AIS、并发缓存、节流边界、数据源校验 |
| `smartship-edge/source/uploader` | 9 | 增量断点、快照指纹、消息 ID、数据源连接键 |
| `smartship-edge/source/monitor` | 4 | 分船数据源校验与连接键 |
| `recovered-source/djys-iot-admin` | 7 | TCP 完整帧读取、半帧 EOF、轮询帧边界、生命周期 |
| **合计** | **41** | 当前四个核心模块的关键边界 |

测试数量只能证明存在这些自动化用例，不等于生产环境吞吐、长稳运行或测试覆盖率。简历可以写“相关模块 41 项测试通过”，不要写“覆盖率 100%”。

## 6.2 推荐讲解顺序

1. 先讲多协议接入和 TCP/NMEA/Modbus 帧边界，证明网络与协议基础。
2. 再讲 12 类 Collector 的生命周期重构和完整帧读取，证明 Java 并发与工程治理能力。
3. 接着讲分船动态数据源和节流修复，证明数据库、并发控制与问题定位能力。
4. 最后讲 16 路弱网上报，主动说明 QoS 1、指纹、幂等和 Outbox 的边界。

## 6.3 面试回答结构

每个问题按以下顺序回答，避免只背技术名词：

1. **现象/约束**：现场数据为什么会出现这个问题。
2. **原因**：协议、线程、数据库或分布式语义上的根因。
3. **实现**：指出具体类、状态或数据结构。
4. **验证**：说明对应测试或可复现实验。
5. **边界**：当前实现没有解决什么，下一步怎么演进。

