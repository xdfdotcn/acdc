package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorTopicMangerService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@Transactional
public class ConnectorTopicManagerServiceImpl implements ConnectorTopicMangerService {

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaHelperService kafkaHelperService;

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    @Override
    public void createTopicIfAbsent(
            final String topic,
            final int partitions,
            final short replicationFactor,
            final Map<String, String> topicConfig) {
        KafkaClusterDO kafkaCluster = kafkaClusterService.findInnerKafkaCluster()
                .orElseThrow(() -> new NotFoundException(String.format("type: %s", KafkaClusterType.INNER)));
        Map<String, Object> adminConfig = kafkaClusterProcessService.getAdminConfig(kafkaCluster.getId());

        kafkaHelperService.createTopic(
                topic,
                partitions,
                replicationFactor,
                topicConfig,
                adminConfig
        );
    }
}
