package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.repository.HiveTableRepository;
import cn.xdf.acdc.devops.service.entity.HiveTableService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class HiveTableServiceImpl implements HiveTableService {

    @Autowired
    private HiveTableRepository hiveTableRepository;

    @Override

    public HiveTableDO save(final HiveTableDO hiveTable) {
        return hiveTableRepository.save(hiveTable);
    }

    @Override
    public List<HiveTableDO> saveAll(final List<HiveTableDO> hiveTableList) {
        return hiveTableRepository.saveAll(hiveTableList);
    }

    @Override
    public Page<HiveTableDO> query(final HiveTableDO hiveTable, final Pageable pageable) {
        return hiveTableRepository.findAll(HiveTableService.specificationOf(hiveTable), pageable);
    }

    @Override
    public List<HiveTableDO> queryAll(final HiveTableDO hiveTable) {
        return hiveTableRepository.findAll(HiveTableService.specificationOf(hiveTable));
    }

    @Override
    public Optional<HiveTableDO> findById(final Long id) {
        return hiveTableRepository.findById(id);
    }

    @Override
    public List<HiveTableDO> findAllById(final Iterable<Long> ids) {
        return hiveTableRepository.findAllById(ids);
    }
}
