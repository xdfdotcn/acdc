package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.repository.RdbTableRepository;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class RdbTableServiceImpl implements RdbTableService {

    @Autowired
    private RdbTableRepository rdbTableRepository;

    @Override
    public RdbTableDO save(final RdbTableDO rdbTable) {
        return rdbTableRepository.save(rdbTable);
    }

    @Override
    public List<RdbTableDO> saveAll(final List<RdbTableDO> rdbTableList) {
        return rdbTableRepository.saveAll(rdbTableList);
    }

    @Override
    public Page<RdbTableDO> query(final RdbTableDO rdbTable, final Pageable pageable) {
        return rdbTableRepository.findAll(RdbTableService.specificationOf(rdbTable), pageable);
    }

    @Override
    public List<RdbTableDO> queryAll(final RdbTableDO rdbTable) {
        return rdbTableRepository.findAll(RdbTableService.specificationOf(rdbTable));
    }

    @Override
    public Optional<RdbTableDO> findById(final Long id) {
        return rdbTableRepository.findById(id);
    }

    @Override
    public List<RdbTableDO> findAllById(final Iterable<Long> ids) {
        return rdbTableRepository.findAllById(ids);
    }
}
