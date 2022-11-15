package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.repository.HiveDatabaseRepository;
import cn.xdf.acdc.devops.service.entity.HiveDatabaseService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class HiveDatabaseServiceImpl implements HiveDatabaseService {

    @Autowired
    private HiveDatabaseRepository hiveDatabaseRepository;

    @Override
    public HiveDatabaseDO save(final HiveDatabaseDO hiveDatabase) {
        return hiveDatabaseRepository.save(hiveDatabase);
    }

    @Override
    public List<HiveDatabaseDO> saveAll(final List<HiveDatabaseDO> hiveDatabaseList) {
        return hiveDatabaseRepository.saveAll(hiveDatabaseList);
    }

    @Override
    public Page<HiveDatabaseDO> query(final HiveDatabaseDO hiveDatabase, final Pageable pageable) {
        return hiveDatabaseRepository.findAll(HiveDatabaseService.specificationOf(hiveDatabase), pageable);
    }

    @Override
    public List<HiveDatabaseDO> queryAll(final HiveDatabaseDO hiveDatabase) {
        return hiveDatabaseRepository.findAll(HiveDatabaseService.specificationOf(hiveDatabase));
    }

    @Override
    public Optional<HiveDatabaseDO> findById(final Long hiveDatabaseId) {
        return hiveDatabaseRepository.findById(hiveDatabaseId);
    }
}
