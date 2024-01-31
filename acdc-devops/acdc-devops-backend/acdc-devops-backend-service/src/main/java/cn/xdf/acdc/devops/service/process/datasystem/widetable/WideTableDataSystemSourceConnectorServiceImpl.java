package cn.xdf.acdc.devops.service.process.datasystem.widetable;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.widetable.WideTableConstant.Connector.Source.Configuration;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class WideTableDataSystemSourceConnectorServiceImpl implements DataSystemSourceConnectorService {
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private ConnectorClassService connectorClassService;
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.ACDC_WIDE_TABLE;
    }
    
    @Override
    public void verifyDataSystemMetadata(final Long dataCollectionId) {
        // keep empty
    }
    
    @Override
    public void beforeConnectorCreation(final Long tableId) {
        // keep empty
    }
    
    @Override
    public void afterConnectorCreation(final Long dataCollectionId) {
        // keep empty
    }
    
    @Override
    public String generateConnectorName(final Long tableId) {
        DataSystemResourceDTO wideTable = dataSystemResourceService.getById(tableId);
        return Joiner.on(CommonConstant.CABLE).join(Configuration.CONNECTOR_NAME_PREFIX, wideTable.getId(), wideTable.getName());
    }
    
    @Override
    public String generateKafkaTopicName(final Long tableId) {
        return generateConnectorName(tableId);
    }
    
    // 1. 监听topics和内部表信息；
    // 2. inner starrocks 和 kafka 信息；
    // 3. 获取sql拓扑；
    // 4. topic名称；
    // 5. 任务唯一标识；
    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final List<Long> dataCollectionIds) {
        
        return null;
    }
    
    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.ACDC_WIDE_TABLE, ConnectorType.SOURCE);
        
        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }
    
    @Override
    public Set<String> getImmutableConfigurationNames() {
        return new HashSet<>();
    }
    
    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return new HashSet<>();
    }
    
    @Override
    public DataSystemResourceType getConnectorDataSystemResourceType() {
        return DataSystemResourceType.ACDC_WIDE_TABLE;
    }
    
    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.ACDC_WIDE_TABLE, ConnectorType.SOURCE);
    }
}
