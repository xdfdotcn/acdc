# 目的
在 acdc 内形成统一的描述体系，降低理解、沟通成本

本词汇表随着项目的发展不断丰富

# 词典表
## kafka connect 领域

 | 单词 | 描述|
 | ---- | ---- |
 | connect | kafka connect 工作进程 |
 | connector | connect 里的同步任务，包含多个 task |
 | task | 实际执行同步任务的线程，有 connect 管理其生命周期 |
 | source | 数据源 |
 | sink | 数据目标 |
 
## 数据链路领域

 | 单词 | 描述|
 | --- | --- |
 | destination | 数据同步目标，语义几乎等同于 sink data set|
 | connection | 一条数据链路，描述了从一个 data system 的 data set 到另一个 data system 的 data set |
 | stream connection | 流式链路，主要用于增量同步 |
 | batch connection | 批式链路，主要用户全量同步 |
 | entire synchronization | 全量同步 |
 | incremental synchronization | 增量同步 |
 
## 数据系统领域

 | 单词 | 描述|
 | --- | --- |
 | project | 业务项目，与 data system 多对多映射 |
 | data system | 数据系统，指上下游的 data set 拥有者 |
 | data set | 数据集，不同 data system 可能代表不同的实体：table、topic 等 |
 | data schema | 数据空间，类似关系型数据库 database 的概念，表示一个表的命名空间 |