package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.repository.HiveRepository;
import cn.xdf.acdc.devops.service.entity.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class HiveServiceImpl implements HiveService {

    @Autowired
    private HiveRepository hiveRepository;

    @Override
    public HiveDO save(final HiveDO hive) {
        return hiveRepository.saveAndFlush(hive);
    }

    @Override
    public Optional<HiveDO> findById(final Long id) {
        return hiveRepository.findById(id);
    }

    @Override
    public List<HiveDO> saveAll(final List<HiveDO> hiveList) {
        return hiveRepository.saveAll(hiveList);
    }
}
