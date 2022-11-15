package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class KafkaClusterServiceImpl implements KafkaClusterService {

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Override
    public KafkaClusterDO save(final KafkaClusterDO kafkaCluster) {
        return kafkaClusterRepository.save(kafkaCluster);
    }

    @Override
    public List<KafkaClusterDO> findAll() {
        return kafkaClusterRepository.findAll();
    }

    @Override
    public Optional<KafkaClusterDO> findById(final Long id) {
        return kafkaClusterRepository.findById(id);
    }

    @Override
    public Optional<KafkaClusterDO> findInnerKafkaCluster() {
        return kafkaClusterRepository.findByClusterType(KafkaClusterType.INNER);
    }

    @Override
    public Optional<KafkaClusterDO> findTicdcKafkaCluster() {
        return kafkaClusterRepository.findByClusterType(KafkaClusterType.TICDC);
    }
}
