package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.HiveDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveDbMetaDTO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.query.HiveDatabaseQuery;
import cn.xdf.acdc.devops.core.domain.query.HiveTableQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.HiveDatabaseRepository;
import cn.xdf.acdc.devops.repository.HiveRepository;
import cn.xdf.acdc.devops.repository.HiveTableRepository;
import cn.xdf.acdc.devops.service.entity.HiveDatabaseService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDatabaseProcessService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HiveHelperService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import javax.persistence.EntityNotFoundException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
@Slf4j
@Order(20)
public class HiveDatabaseProcessServiceImpl implements HiveDatabaseProcessService, DataSystemMetadataService<HiveTableDO> {

    private static final Long DEFAULT_HIVE_ID = 1L;

    @Autowired
    private HiveHelperService hiveHelperService;

    @Autowired
    private HiveDatabaseService hiveDatabaseService;

    @Autowired
    private HiveTableRepository hiveTableRepository;

    @Autowired
    private HiveRepository hiveRepository;

    @Autowired
    private HiveDatabaseRepository hiveDatabaseRepository;

    @Override
    public Page<HiveDatabaseDTO> queryHiveDatabase(final HiveDatabaseDTO hiveDatabaseDTO) {
        HiveDatabaseDO query = HiveDatabaseDO.builder()
                .name(hiveDatabaseDTO.getName())
                .hive(HiveDO.builder()
                        .id(hiveDatabaseDTO.getClusterId())
                        .build())
                .build();

        Pageable pageable = PagedQuery.ofPage(hiveDatabaseDTO.getCurrent(), hiveDatabaseDTO.getPageSize());
        return hiveDatabaseService.query(query, pageable).map(HiveDatabaseDTO::new);
    }

    @Override
    public HiveDatabaseDTO getHiveDatabase(final Long id) {
        return hiveDatabaseRepository.findById(id)
                .map(HiveDatabaseDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public void refreshMetadata(final List<HiveTableDO> freshElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refreshMetadata() {
        // TODO 是否需要分页diffing,database count: 200+,table count: 1w+

        log.info("Prepare sync meta data from hive");
        StopWatch watch = new StopWatch();
        watch.start();

        // 1. 从hive 的mysql 中拉去数据库和数据表的元数据信息
        Map<String, Set<String>> originMetaHiveTableMap = fetchOriginMetaHiveTable();

        // 2. 从 DB中获取所有的 hive 数据库和数据表
        Map<String, Pair<Long, List<HiveTableDO>>> originDbHiveTableMap = fetchOriginDbHiveTable();

        // 3. 删除DB中多于hive元数据中的库和表
        diffingDataToDeleteIfNeeded(originMetaHiveTableMap, originDbHiveTableMap);

        // 4. 增加hive元数据多于DB中的库和表到DB中
        diffingDataToAddIfNeeded(originMetaHiveTableMap, originDbHiveTableMap);

        originDbHiveTableMap.clear();
        originMetaHiveTableMap.clear();

        watch.stop();

        log.info("Sync meta data from hive end cost: {} ms", watch.getTotalTimeMillis());
    }

    private void diffingDataToDeleteIfNeeded(
            final Map<String, Set<String>> originMetaHiveTableMap,
            final Map<String, Pair<Long, List<HiveTableDO>>> originDbHiveTableMap) {

        HiveDO hive = hiveRepository.findById(DEFAULT_HIVE_ID)
                .orElseThrow(() -> new EntityNotFoundException(String.format("hiveId: %s", DEFAULT_HIVE_ID)));

        // 需要这整库删除的表
        Map<String, Pair<Long, List<HiveTableDO>>> wholeDatabaseToDeleteHiveTableMap = Maps.newHashMap();

        // 需要删除的表
        List<Pair<Long, HiveTableDO>> commonToDeleteHiveTableList = Lists.newArrayList();

        for (Entry<String, Pair<Long, List<HiveTableDO>>> entry : originDbHiveTableMap.entrySet()) {
            String dbHiveDatabaseName = entry.getKey();
            Long dbHiveDatabaseId = entry.getValue().getKey();
            List<HiveTableDO> dbHiveTables = entry.getValue().getValue();
            Set<String> metaHiveTables = originMetaHiveTableMap.get(dbHiveDatabaseName);

            // hive 元数据中没有对应的库,需要删除此库和此库对应的表
            if (Objects.isNull(metaHiveTables)) {
                wholeDatabaseToDeleteHiveTableMap.put(dbHiveDatabaseName, entry.getValue());
                continue;
            }

            // hive 元数据中存在对应的库,但是没有对应的表,需要删除对应库下对应的表
            for (HiveTableDO dbHiveTable : dbHiveTables) {
                if (!metaHiveTables.contains(dbHiveTable.getName())) {
                    commonToDeleteHiveTableList.add(Pair.of(dbHiveDatabaseId, dbHiveTable));
                }
            }
        }

        List<HiveDatabaseDO> toSaveHiveDatabaseList = Lists.newArrayList();
        List<HiveTableDO> toSaveHiveTableList = Lists.newArrayList();

        // 1. 处理需要删除的数据库
        for (Entry<String, Pair<Long, List<HiveTableDO>>> entry : wholeDatabaseToDeleteHiveTableMap.entrySet()) {
            String hiveDatabaseName = entry.getKey();
            Long hiveDatabaseId = entry.getValue().getKey();
            HiveDatabaseDO newHiveDatabase = HiveDatabaseDO.builder()
                    .id(hiveDatabaseId)
                    .hive(HiveDO.builder()
                            .id(hive.getId())
                            .build())
                    .name(hiveDatabaseName)
                    .build();
            toSaveHiveDatabaseList.add(newHiveDatabase);
        }

        // 2. 处理需要整库删除的表
        for (Entry<String, Pair<Long, List<HiveTableDO>>> entry : wholeDatabaseToDeleteHiveTableMap.entrySet()) {
            Long hiveDatabaseId = entry.getValue().getKey();
            List<HiveTableDO> toDelHiveTables = entry.getValue().getValue();
            for (HiveTableDO toDelHiveTable : toDelHiveTables) {
                HiveTableDO newHiveTable = cloneHiveTableDOAndMarkToDeleted(toDelHiveTable, hiveDatabaseId);
                toSaveHiveTableList.add(newHiveTable);
            }
        }

        // 3. 处理需要删除的表
        for (Pair<Long, HiveTableDO> entry : commonToDeleteHiveTableList) {
            Long databaseId = entry.getKey();
            HiveTableDO toDelHiveTable = entry.getValue();
            HiveTableDO newHiveTable = cloneHiveTableDOAndMarkToDeleted(toDelHiveTable, databaseId);
            toSaveHiveTableList.add(newHiveTable);
        }

        // 4. 批量更新
        hiveDatabaseRepository.saveAll(toSaveHiveDatabaseList);
        hiveTableRepository.saveAll(toSaveHiveTableList);

        // help GC
        toSaveHiveDatabaseList.clear();
        toSaveHiveTableList.clear();
        wholeDatabaseToDeleteHiveTableMap.clear();
        commonToDeleteHiveTableList.clear();
    }

    private HiveTableDO cloneHiveTableDOAndMarkToDeleted(final HiveTableDO hiveTableDO, final Long hiveDatabaseId) {
        return HiveTableDO.builder()
                .id(hiveTableDO.getId())
                .hiveDatabase(HiveDatabaseDO.builder()
                        .id(hiveDatabaseId)
                        .build())
                .name(hiveTableDO.getName())
                .deleted(Boolean.TRUE)
                .build();
    }

    private void diffingDataToAddIfNeeded(
            final Map<String, Set<String>> originHiveTableMetaMap,
            final Map<String, Pair<Long, List<HiveTableDO>>> originDbHiveTableMap) {
        HiveDO hive = hiveRepository.findById(DEFAULT_HIVE_ID)
                .orElseThrow(() -> new EntityNotFoundException(String.format("hiveId: %s", DEFAULT_HIVE_ID)));

        // 需要整库添加的表
        Map<String, Set<String>> wholeDatabaseToAddHiveTableMap = Maps.newHashMap();

        // 需要添加的表
        List<Entry<Long, String>> commonToAddHiveTableList = Lists.newArrayList();

        // 1. 遍历每个库,找出需要新增的表,和需要整库添加的表
        for (Entry<String, Set<String>> entry : originHiveTableMetaMap.entrySet()) {
            String metaHiveDatabaseName = entry.getKey();
            Set<String> metaHiveTables = entry.getValue();

            Pair<Long, List<HiveTableDO>> dbHiveTableEntry = originDbHiveTableMap.get(metaHiveDatabaseName);

            // Db 中没有对应的数据库,需要整库添加表
            if (Objects.isNull(dbHiveTableEntry)) {
                wholeDatabaseToAddHiveTableMap.put(metaHiveDatabaseName, entry.getValue());
                continue;
            }

            // Db 中存在对应的库,但是不存在库下对应的表,则新增表
            Set<String> dbHiveTables = dbHiveTableEntry.getValue().stream()
                    .map(HiveTableDO::getName)
                    .collect(Collectors.toSet());

            Long dbHiveDatabaseId = dbHiveTableEntry.getKey();
            for (String metaHiveTable : metaHiveTables) {
                if (!dbHiveTables.contains(metaHiveTable)) {
                    commonToAddHiveTableList.add(new SimpleEntry(dbHiveDatabaseId, metaHiveTable));
                }
            }
        }

        List<HiveDatabaseDO> toSaveHiveDatabaseList = Lists.newArrayList();
        List<HiveTableDO> toSaveHiveTableList = Lists.newArrayList();

        // 2. 增加数据库
        for (Entry<String, Set<String>> entry : wholeDatabaseToAddHiveTableMap.entrySet()) {
            String hiveDatabaseName = entry.getKey();
            HiveDatabaseDO newHiveDatabase = HiveDatabaseDO.builder()
                    .hive(HiveDO.builder()
                            .id(hive.getId())
                            .build())
                    .name(hiveDatabaseName)
                    .build();
            toSaveHiveDatabaseList.add(newHiveDatabase);
        }
        List<HiveDatabaseDO> savedHiveDatabaseList = hiveDatabaseRepository.saveAll(toSaveHiveDatabaseList);

        //3. 增加需要整库同步的表
        for (HiveDatabaseDO savedHiveDatabase : savedHiveDatabaseList) {
            Set<String> toAddHiveTables = wholeDatabaseToAddHiveTableMap.get(savedHiveDatabase.getName());
            Long savedHiveDatabaseId = savedHiveDatabase.getId();
            for (String toAddHiveTable : toAddHiveTables) {
                HiveTableDO newHiveTable = HiveTableDO.builder()
                        .name(toAddHiveTable)
                        .hiveDatabase(HiveDatabaseDO.builder()
                                .id(savedHiveDatabaseId)
                                .build())
                        .build();
                toSaveHiveTableList.add(newHiveTable);
            }
        }

        // 4.处理需要增加的表
        for (Entry<Long, String> entry : commonToAddHiveTableList) {
            Long hiveDatabaseId = entry.getKey();
            String hiveTable = entry.getValue();
            HiveTableDO newHiveTable = HiveTableDO.builder()
                    .name(hiveTable)
                    .hiveDatabase(HiveDatabaseDO.builder()
                            .id(hiveDatabaseId)
                            .build())
                    .build();
            toSaveHiveTableList.add(newHiveTable);
        }

        // 5. 保存需要增加的 hive 表
        hiveTableRepository.saveAll(toSaveHiveTableList);

        toSaveHiveDatabaseList.clear();
        toSaveHiveTableList.clear();
        wholeDatabaseToAddHiveTableMap.clear();
        commonToAddHiveTableList.clear();
    }

    /**
     * 查询数据库中所有的 hive 表.
     */
    private Map<String, Pair<Long, List<HiveTableDO>>> fetchOriginDbHiveTable() {

        Map<String, Pair<Long, List<HiveTableDO>>> hiveTableMap = new HashMap<>();

        Map<Long, String> databaseMap = hiveDatabaseRepository
                .query(HiveDatabaseQuery.builder()
                        .deleted(false)
                        .build())
                .stream()
                .collect(Collectors.toMap(HiveDatabaseDO::getId, HiveDatabaseDO::getName));

        hiveTableRepository
                .queryAll(HiveTableQuery.builder()
                        .deleted(false)
                        .build())
                .forEach(it -> {
                    String databaseName = databaseMap.get(it.getHiveDatabase().getId());
                    Long databaseId = it.getHiveDatabase().getId();

                    Pair<Long, List<HiveTableDO>> entry = hiveTableMap.computeIfAbsent(databaseName, key -> Pair.of(databaseId, Lists.newArrayList()));
                    entry.getValue().add(it);
                });

        return hiveTableMap;
    }

    private Map<String, Set<String>> fetchOriginMetaHiveTable() {
        List<HiveDbMetaDTO> metas = hiveHelperService.fetchHiveDbMeta();
        return metas.stream().collect(
                Collectors.groupingBy(HiveDbMetaDTO::getDb,
                        Collectors.mapping(HiveDbMetaDTO::getTable, Collectors.toSet()))
        );
    }
}
