package cn.xdf.acdc.devops.service.process.connector;

import java.util.Map;

public interface ConnectorTopicMangerService {
    
    /**
     * 创建 kafka topic.
     *
     * @param topic topic
     * @param partitions 分区数量
     * @param replicationFactor 副本数量
     * @param topicConfig topic配置信息
     */
    void createTopicIfAbsent(String topic, int partitions, short replicationFactor, Map<String, String> topicConfig);
}
