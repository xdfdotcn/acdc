package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.RdbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RdbServiceImpl implements RdbService {

    @Autowired
    private RdbRepository rdbRepository;

    @Override
    public RdbDO save(final RdbDO rdb) {
        return rdbRepository.save(rdb);
    }

    @Override
    public List<RdbDO> saveAll(final List<RdbDO> rdbList) {
        return rdbRepository.saveAll(rdbList);
    }

    @Override
    public Optional<RdbDO> findById(final Long id) {
        return rdbRepository.findById(id);
    }
}
