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
        // TODO ??????????????????diffing,database count: 200+,table count: 1w+

        log.info("Prepare sync meta data from hive");
        StopWatch watch = new StopWatch();
        watch.start();

        // 1. ???hive ???mysql ????????????????????????????????????????????????
        Map<String, Set<String>> originMetaHiveTableMap = fetchOriginMetaHiveTable();

        // 2. ??? DB?????????????????? hive ?????????????????????
        Map<String, Pair<Long, List<HiveTableDO>>> originDbHiveTableMap = fetchOriginDbHiveTable();

        // 3. ??????DB?????????hive????????????????????????
        diffingDataToDeleteIfNeeded(originMetaHiveTableMap, originDbHiveTableMap);

        // 4. ??????hive???????????????DB??????????????????DB???
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

        // ???????????????????????????
        Map<String, Pair<Long, List<HiveTableDO>>> wholeDatabaseToDeleteHiveTableMap = Maps.newHashMap();

        // ??????????????????
        List<Pair<Long, HiveTableDO>> commonToDeleteHiveTableList = Lists.newArrayList();

        for (Entry<String, Pair<Long, List<HiveTableDO>>> entry : originDbHiveTableMap.entrySet()) {
            String dbHiveDatabaseName = entry.getKey();
            Long dbHiveDatabaseId = entry.getValue().getKey();
            List<HiveTableDO> dbHiveTables = entry.getValue().getValue();
            Set<String> metaHiveTables = originMetaHiveTableMap.get(dbHiveDatabaseName);

            // hive ??????????????????????????????,???????????????????????????????????????
            if (Objects.isNull(metaHiveTables)) {
                wholeDatabaseToDeleteHiveTableMap.put(dbHiveDatabaseName, entry.getValue());
                continue;
            }

            // hive ??????????????????????????????,????????????????????????,????????????????????????????????????
            for (HiveTableDO dbHiveTable : dbHiveTables) {
                if (!metaHiveTables.contains(dbHiveTable.getName())) {
                    commonToDeleteHiveTableList.add(Pair.of(dbHiveDatabaseId, dbHiveTable));
                }
            }
        }

        List<HiveDatabaseDO> toSaveHiveDatabaseList = Lists.newArrayList();
        List<HiveTableDO> toSaveHiveTableList = Lists.newArrayList();

        // 1. ??????????????????????????????
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

        // 2. ??????????????????????????????
        for (Entry<String, Pair<Long, List<HiveTableDO>>> entry : wholeDatabaseToDeleteHiveTableMap.entrySet()) {
            Long hiveDatabaseId = entry.getValue().getKey();
            List<HiveTableDO> toDelHiveTables = entry.getValue().getValue();
            for (HiveTableDO toDelHiveTable : toDelHiveTables) {
                HiveTableDO newHiveTable = cloneHiveTableDOAndMarkToDeleted(toDelHiveTable, hiveDatabaseId);
                toSaveHiveTableList.add(newHiveTable);
            }
        }

        // 3. ????????????????????????
        for (Pair<Long, HiveTableDO> entry : commonToDeleteHiveTableList) {
            Long databaseId = entry.getKey();
            HiveTableDO toDelHiveTable = entry.getValue();
            HiveTableDO newHiveTable = cloneHiveTableDOAndMarkToDeleted(toDelHiveTable, databaseId);
            toSaveHiveTableList.add(newHiveTable);
        }

        // 4. ????????????
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

        // ????????????????????????
        Map<String, Set<String>> wholeDatabaseToAddHiveTableMap = Maps.newHashMap();

        // ??????????????????
        List<Entry<Long, String>> commonToAddHiveTableList = Lists.newArrayList();

        // 1. ???????????????,????????????????????????,???????????????????????????
        for (Entry<String, Set<String>> entry : originHiveTableMetaMap.entrySet()) {
            String metaHiveDatabaseName = entry.getKey();
            Set<String> metaHiveTables = entry.getValue();

            Pair<Long, List<HiveTableDO>> dbHiveTableEntry = originDbHiveTableMap.get(metaHiveDatabaseName);

            // Db ???????????????????????????,?????????????????????
            if (Objects.isNull(dbHiveTableEntry)) {
                wholeDatabaseToAddHiveTableMap.put(metaHiveDatabaseName, entry.getValue());
                continue;
            }

            // Db ?????????????????????,?????????????????????????????????,????????????
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

        // 2. ???????????????
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

        //3. ??????????????????????????????
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

        // 4.????????????????????????
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

        // 5. ????????????????????? hive ???
        hiveTableRepository.saveAll(toSaveHiveTableList);

        toSaveHiveDatabaseList.clear();
        toSaveHiveTableList.clear();
        wholeDatabaseToAddHiveTableMap.clear();
        commonToAddHiveTableList.clear();
    }

    /**
     * ??????????????????????????? hive ???.
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
