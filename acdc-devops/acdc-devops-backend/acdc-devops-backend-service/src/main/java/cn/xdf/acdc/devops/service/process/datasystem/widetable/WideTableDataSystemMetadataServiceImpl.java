package cn.xdf.acdc.devops.service.process.datasystem.widetable;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableColumnDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemConstant.Metadata.StarRocks;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WideTableDataSystemMetadataServiceImpl implements DataSystemMetadataService {
    
    @Autowired
    private WideTableService wideTableService;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return WideTableDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    public DataCollectionDefinition getDataCollectionDefinition(final Long dataCollectionId) {
        // todo
        WideTableQuery query = new WideTableQuery()
                .setDataCollectionId(dataCollectionId)
                .setDeleted(false);
        List<WideTableDetailDTO> wideTables = wideTableService.queryDetail(query);
        assert wideTables.size() == 1;
        WideTableDetailDTO wideTableDetail = wideTables.get(0);
        List<DataFieldDefinition> fields = wideTableDetail.getWideTableColumns().stream()
                .map(this::getDataFieldDefinition)
                .collect(Collectors.toList());
        return new DataCollectionDefinition(wideTableDetail.getName(), fields);
    }
    
    private DataFieldDefinition getDataFieldDefinition(final WideTableColumnDTO column) {
        Set<String> uniqueIndexNames = new HashSet<>();
        if (column.getIsPrimaryKey()) {
            uniqueIndexNames.add(StarRocks.PK_INDEX_NAME);
        }
        return new DataFieldDefinition(column.getType(), column.getType(), uniqueIndexNames);
    }
    
    @Override
    public DataSystemResourceDTO createDataCollectionByDataDefinition(final Long parentId, final String dataCollectionName, final DataCollectionDefinition dataCollectionDefinition) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void checkDataSystem(final Long rootDataSystemResourceId) {
        // keep empty
    }
    
    @Override
    public void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        // keep empty
    }
    
    @Override
    public void refreshDynamicDataSystemResource(final Long rootDataSystemResourceId) {
        // keep empty
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.ACDC_WIDE_TABLE;
    }
}
