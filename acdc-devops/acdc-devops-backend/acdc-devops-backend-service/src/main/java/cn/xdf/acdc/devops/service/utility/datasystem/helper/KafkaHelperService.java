package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import org.apache.kafka.common.acl.AclOperation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * kafka 客户端操作 Service .
 */

public interface KafkaHelperService {

    /**
     * Create topic.
     *
     * @param topicName         topic name
     * @param numPartitions     partition number
     * @param replicationFactor replication factor
     * @param topicConfig       topic config
     * @param adminConfig       kafka admin config
     */
    void createTopic(String topicName, int numPartitions, short replicationFactor, Map<String, String> topicConfig, Map<String, Object> adminConfig);

    /**
     * Delete topics.
     *
     * @param adminConfig kafka admin config
     * @param topics      topic names
     */
    void deleteTopics(List<String> topics, Map<String, Object> adminConfig);

    /**
     * Add topic acl for user.
     *
     * @param topic        topic name
     * @param userName     user name
     * @param aclOperation AclOperation.WRITE, AclOperation.READ, AclOperation.CREATE, AclOperation.DELETE etc.
     * @param adminConfig  kafka admin config
     */
    void addAcl(String topic, String userName, AclOperation aclOperation, Map<String, Object> adminConfig);

    /**
     * Delete topic acl for user.
     *
     * @param adminConfig kafka admin config
     * @param topic       topic name
     * @param userName    user name
     */
    void deleteAcl(String topic, String userName, Map<String, Object> adminConfig);

    /**
     * query all kafka topic.
     *
     * @param adminConfig kafka admin config
     * @return java.util.Set
     * @date 2022/9/2 5:41 下午
     */
    Set<String> listTopics(Map<String, Object> adminConfig);

    /**
     * check kafka client config.
     *
     * @param bootstrapServers bootstrap server addresses
     * @param config           config
     * @date 2022/9/16 4:43 下午
     */
    void checkConfig(String bootstrapServers, Map<String, Object> config);
}
