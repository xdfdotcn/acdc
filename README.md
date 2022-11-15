# 产品定义

![image](https://user-images.githubusercontent.com/118262561/205564974-ba3a2723-7442-4602-803a-6a58fcc23802.png)

ACDC，A Change Data Capture，是新东方集团架构部开源的数据平台产品。目标是成为一个完整的数据中台解决方案，为大数据团队和研发团队提供以下能力：
1. 端到端批、流式数据同步（已部分实现，详见 [当前能力](#当前能力) ）
2. 数据处理
3. 数据服务

# 产品由来

新东方的一些核心业务存在单元写、中心入仓的场景，因此需要将数据从各单元的关系型数据库同步到中心，并异构存储到数据仓库之中。
技术团队最初使用 Apache Sqoop 以批的方式实现了这个能力。随着数据量的增长，这个方案很快暴露出了一些问题，如：

1. 为了不影响业务，同步数据只能在夜间进行，制约了报表的时效性
2. 数据的同步周期随着数据量增长而增长

通过引入 kafka connect 技术栈，并结合 Canal、SQLServer CT 等工具，实现了从批到流的转变，有效解决了以上问题。这时，随着数据链路的数量不断增长，又暴露出了一些新的问题，如：
1. 无 DevOps 手段，需要专属团队统一运维，边际成本较高且效率较低
2. 血缘关系只能依靠文档记录，数据溯源的成本随着时间推移而提升
3. 随着租户身份不断增多，需要精细的监控、告警手段
4. 缺乏数据权限管理手段，仍需借助 OA 等外部系统

新东方集团架构部决定以平台化方式解决上述问题，并将此产品逐渐演进为完整的数据中台解决方案，这个产品就是 ACDC。

# 使用场景

1. 单元写、中心入仓
2. 数据异构存储
3. 捕获数据事件

# 当前能力

ACDC 目前处于 1.0 版本，并在持续迭代中。当前版本的主要功能是以多租户及 DevOps 方式管理流式数据同步链路，具体功能点如下：
1. 多租户
2. 端到端数据流式同步链路生命周期管理
3. 数据权限审批
4. 链路、平台粒度的可观测性（基于 prometheus + grafana）
5. 部分元数据白屏化管理

## 支持的数据系统

| 数据源 | 数据目标 |
| ---- | ---- |
| MySQL <br> TiDB（with TiCDC） | JDBC 支持的数据系统（MySQL、TiDB、SQLServer、Oracle 等）<br> Hive <br> Kafka |

# 产品发展规划

ACDC 会在数据同步、数据处理、数据服务三个方面持续发展。
数据同步方面，主要是针对批、流两种同步方式支持更多的数据系统，预期如下：

| 状态 | 数据源 | 数据目标 |
| ---- | ---- | ---- |
| 已实现 | MySQL <br> TiDB（with TiCDC） | JDBC 支持的数据系统（MySQL、TiDB、SQLServer、Oracle 等）<br> Hive <br> Kafka |
| 已实现 | TiDB (with TikvClient) <br> Oracle <br> Sqlserver <br> PostgreSQL <br> Kafka <br> Hologres | Elastic Search <br> Redis <br> MacCompute <br> Hologres <br> PostgreSQL <br> StarRocks <br> IceBerg <br> Hudi |

数据处理方面，主要是针对数据提供一些加工、聚合能力，例如数据变换，数据过滤，数据维度打宽等。

数据服务方面，主要是将数据同步、处理的结果提供 Rest 等访问方式。

# 产品架构

// TODO

# 开源依赖情况

## JDBC Sink

ACDC 中的 JDBC Sink 模块基于 confluent 社区开源的 JDBC Sink 进行二次开发，原项目地址如下：
https://github.com/confluentinc/kafka-connect-jdbc

### 注意
原项目中的部分代码已经被 acdc 团队进行了调整

## HDFS Sink

ACDC 中的 HDFS Sink 模块基于 confluent 社区开源的 HDFS Sink 进行二次开发，原项目地址如下：
https://github.com/confluentinc/kafka-connect-hdfs

### 注意
原项目中的部分代码已经被 acdc 团队进行了调整

## MySQL Source

ACDC 中的 MySQL Source 模块使用由 redhat 主导的开源项目 debezium，原项目地址如下：
https://debezium.io/

# 了解更多

请通过提交 issues 的方式获取您想了解的更多细节。
