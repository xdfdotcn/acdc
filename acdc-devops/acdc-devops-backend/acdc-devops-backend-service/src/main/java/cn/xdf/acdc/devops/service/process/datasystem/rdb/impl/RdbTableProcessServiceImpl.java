package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.RdbTableDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.RdbTableRepository;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbTableProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;

@Service
@Transactional
public class RdbTableProcessServiceImpl implements RdbTableProcessService {

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private RdbTableRepository rdbTableRepository;

    @Override
    public Page<RdbTableDTO> queryRdbTable(final RdbTableDTO rdbTableDTO) {

        RdbTableDO query = RdbTableDO.builder()
                .name(rdbTableDTO.getName())
                .rdbDatabase(RdbDatabaseDO.builder().id(rdbTableDTO.getDatabaseId()).build())
                .build();

        Pageable pageable = PagedQuery.ofPage(rdbTableDTO.getCurrent(), rdbTableDTO.getPageSize());
        return rdbTableService.query(query, pageable).map(RdbTableDTO::new);
    }

    @Override
    public RdbTableDTO getRdbTable(final Long id) {
        return rdbTableRepository.findById(id)
                .map(RdbTableDTO::toRdbTableDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }
}
