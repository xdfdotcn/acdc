package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.RdbDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.RdbDatabaseRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbDatabaseProcessService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Transactional
@Slf4j
@Order(11)
public class RdbDatabaseProcessServiceImpl implements RdbDatabaseProcessService, DataSystemMetadataService<RdbDatabaseDO> {

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Autowired
    private RdbDatabaseRepository rdbDatabaseRepository;

    @Override
    public Page<RdbDatabaseDTO> queryRdbDatabase(final RdbDatabaseDTO rdbDatabaseDTO) {
        RdbDatabaseDO query = RdbDatabaseDO.builder()
                .name(rdbDatabaseDTO.getName())
                .rdb(new RdbDO().builder().id(rdbDatabaseDTO.getClusterId()).build())
                .build();

        Pageable pageable = PagedQuery.ofPage(rdbDatabaseDTO.getCurrent(), rdbDatabaseDTO.getPageSize());
        return rdbDatabaseService.query(query, pageable).map(RdbDatabaseDTO::new);
    }

    @Override
    public RdbDatabaseDTO getRdbDatabase(final Long id) {
        return rdbDatabaseRepository.findById(id)
                .map(RdbDatabaseDTO::toRdbDatabaseDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public void refreshMetadata() {
        List<RdbDatabaseDO> dbs = rdbDatabaseService.queryAll(new RdbDatabaseDO());
        refreshMetadata(dbs);
    }

    @Override
    public void refreshMetadata(final List<RdbDatabaseDO> dbs) {
        if (Collections.isEmpty(dbs)) {
            return;
        }

        dbs.forEach(database -> {
            RdbDO rdb = rdbRepository.findById(database.getRdb().getId())
                    .orElseThrow(() -> new EntityNotFoundException(String.format("rdb:%s", database.getRdb().getId())));

            List<String> tableNames = showTables(rdb, database.getName());
            if (!CollectionUtils.isEmpty(tableNames)) {
                diffingRdbTable(tableNames, database.getId());
            }
        });
    }

    private void diffingRdbTable(final List<String> tableNames, final Long databaseId) {
        RdbTableDO query = RdbTableDO.builder()
                .rdbDatabase(RdbDatabaseDO.builder()
                        .id(databaseId)
                        .build())
                .build();

        Map<String, RdbTableDO> dbRdbTableMap = rdbTableService.queryAll(query).stream()
                .collect(Collectors.toMap(RdbTableDO::getName, table -> table));

        // 所有匹配不到的视为新增，修改表名称视为新增
        List<RdbTableDO> toSaveRdbTableList = tableNames.stream()
                .filter(table -> Objects.isNull(dbRdbTableMap.get(table)))
                .collect(Collectors.toList()).stream()
                .map(table -> RdbTableDO.builder()
                        .rdbDatabase(RdbDatabaseDO.builder().id(databaseId).build())
                        .name(table)
                        .build())
                .collect(Collectors.toList());

        rdbTableService.saveAll(toSaveRdbTableList);
    }

    private List<String> showTables(final RdbDO rdb, final String databaseName) {
        try {
            return mysqlHelperService.showTables(rdb, databaseName);
        } catch (ServerErrorException e) {
            log.warn("ShowTables exception, dbType: {}, databaseName: {}, hosts: {}, message: {}", rdb.getRdbType(),
                    databaseName, rdb.getDbInstances(), e.getMessage());
            return new ArrayList<>();
        }
    }
}
