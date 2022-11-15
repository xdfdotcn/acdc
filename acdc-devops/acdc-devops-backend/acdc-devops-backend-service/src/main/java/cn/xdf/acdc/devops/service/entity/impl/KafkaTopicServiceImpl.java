package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.repository.KafkaTopicRepository;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class KafkaTopicServiceImpl implements KafkaTopicService {

    @Autowired
    private KafkaTopicRepository kafkaTopicRepository;

    @Override
    public KafkaTopicDO save(final KafkaTopicDO kafkaTopic) {
        return kafkaTopicRepository.save(kafkaTopic);
    }

    @Override
    public List<KafkaTopicDO> saveAll(final List<KafkaTopicDO> kafkaTopicList) {
        return kafkaTopicRepository.saveAll(kafkaTopicList);
    }

    @Override
    public Page<KafkaTopicDO> query(final KafkaTopicDO kafkaTopic, final Pageable pageable) {
        return kafkaTopicRepository.findAll(KafkaTopicService.specificationOf(kafkaTopic), pageable);
    }

    @Override
    public List<KafkaTopicDO> queryAll(final KafkaTopicDO kafkaTopic) {
        return kafkaTopicRepository.findAll(KafkaTopicService.specificationOf(kafkaTopic));
    }

    @Override
    public Optional<KafkaTopicDO> findById(final Long id) {
        return kafkaTopicRepository.findById(id);
    }

    @Override
    public List<KafkaTopicDO> findAllById(final Iterable<Long> ids) {
        return kafkaTopicRepository.findAllById(ids);
    }

    @Override
    public Optional<KafkaTopicDO> findByKafkaClusterIdAndName(final Long kafkaClusterId, final String name) {
        return kafkaTopicRepository.findByKafkaClusterIdAndName(kafkaClusterId, name);
    }
}
