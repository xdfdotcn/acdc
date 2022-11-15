package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.repository.RdbDatabaseRepository;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class RdbDatabaseServiceImpl implements RdbDatabaseService {

    @Autowired
    private RdbDatabaseRepository rdbDatabaseRepository;

    @Override
    public RdbDatabaseDO save(final RdbDatabaseDO rdbDatabase) {
        return rdbDatabaseRepository.save(rdbDatabase);
    }

    @Override
    public List<RdbDatabaseDO> saveAll(final List<RdbDatabaseDO> rdbDatabaseList) {
        return rdbDatabaseRepository.saveAll(rdbDatabaseList);
    }

    @Override
    public Page<RdbDatabaseDO> query(final RdbDatabaseDO rdbDatabase, final Pageable pageable) {
        return rdbDatabaseRepository.findAll(RdbDatabaseService.specificationOf(rdbDatabase), pageable);
    }

    @Override
    public List<RdbDatabaseDO> queryAll(final RdbDatabaseDO rdbDatabase) {
        return rdbDatabaseRepository.findAll(RdbDatabaseService.specificationOf(rdbDatabase));
    }

    @Override
    public Optional<RdbDatabaseDO> findById(final Long rdbDatabaseId) {
        return rdbDatabaseRepository.findById(rdbDatabaseId);
    }
}
