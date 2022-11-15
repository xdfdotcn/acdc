package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseTidbDO;
import cn.xdf.acdc.devops.repository.RdbDatabaseTidbRepository;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseTidbService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RdbDatabaseTidbServiceImpl implements RdbDatabaseTidbService {

    @Autowired
    private RdbDatabaseTidbRepository rdbDatabaseTidbRepository;

    @Override
    public RdbDatabaseTidbDO save(final RdbDatabaseTidbDO rdbDatabaseTidb) {
        return rdbDatabaseTidbRepository.save(rdbDatabaseTidb);
    }

    @Override
    public List<RdbDatabaseTidbDO> saveAll(final List<RdbDatabaseTidbDO> rdbDatabaseTidbList) {
        return rdbDatabaseTidbRepository.saveAll(rdbDatabaseTidbList);
    }

    @Override
    public Optional<RdbDatabaseTidbDO> findById(final Long id) {
        return rdbDatabaseTidbRepository.findById(id);
    }

    @Override
    public Optional<RdbDatabaseTidbDO> findByRdbDataBaseId(final Long rdbDatabaseId) {
        return rdbDatabaseTidbRepository.findOneByRdbDatabaseId(rdbDatabaseId);
    }

    @Override
    public List<RdbDatabaseTidbDO> findAll() {
        return rdbDatabaseTidbRepository.findAll();
    }
}
