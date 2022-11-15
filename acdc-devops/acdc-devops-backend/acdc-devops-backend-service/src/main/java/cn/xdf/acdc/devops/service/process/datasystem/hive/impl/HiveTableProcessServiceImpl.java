package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.HiveTableDTO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.HiveTableRepository;
import cn.xdf.acdc.devops.service.entity.HiveTableService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveTableProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;

@Service
@Transactional
public class HiveTableProcessServiceImpl implements HiveTableProcessService {

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    private HiveTableRepository hiveTableRepository;

    @Override
    public Page<HiveTableDTO> queryHiveTable(final HiveTableDTO hiveTableDTO) {
        HiveTableDO query = HiveTableDO.builder()
                .name(hiveTableDTO.getName())
                .hiveDatabase(HiveDatabaseDO.builder().id(hiveTableDTO.getDatabaseId()).build())
                .build();

        Pageable pageable = PagedQuery.ofPage(hiveTableDTO.getCurrent(), hiveTableDTO.getPageSize());
        return hiveTableService.query(query, pageable).map(HiveTableDTO::new);
    }

    @Override
    public HiveTableDTO getHiveTable(final Long id) {
        return hiveTableRepository.findById(id)
                .map(HiveTableDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }
}
