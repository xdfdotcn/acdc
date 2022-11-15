package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.DefaultConnectorConfigurationService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorConfigProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

public abstract class AbstractConnectorConfigProcessServiceImpl implements ConnectorConfigProcessService {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private DefaultConnectorConfigurationService defaultConfigService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Override
    public Map<String, String> getEncryptConfig(final Long connectorId) {
        return getConfigWithDefault(connectorId);
    }

    @Override
    public Map<String, String> getDecryptConfig(final Long connectorId) {
        return getDecryptConfigByConfItemSet(connectorId, getEncryptConfigItemSet());
    }

    /**
     * Get encrypt config item set, each subclass must be implemented.
     *
     * @return set
     */
    abstract Set<String> getEncryptConfigItemSet();

    protected Map<String, String> getConfigWithDefault(final Long connectorId) {
        ConnectorDO connector = connectorService.findById(connectorId)
            .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        Long connectorClassId = connector.getConnectorClass().getId();

        Map<String, String> defaultConfMap = defaultConfigService.findByConnectorClassId(connectorClassId).stream()
            .collect(Collectors.toMap(conf -> conf.getName(), conf -> conf.getValue()));

        Map<String, String> connectorConfMap = connectorConfigurationService.findByConnectorId(connectorId).stream()
            .collect(Collectors.toMap(conf -> conf.getName(), conf -> conf.getValue()));

        return mergeConf(defaultConfMap, connectorConfMap);
    }

    protected Map<String, String> getDecryptConfigByConfItemSet(final Long connectorId, final Set<String> confItemSet) {
        Map<String, String> confMap = getConfigWithDefault(connectorId);

        confMap.entrySet().stream()
            .filter(it -> confItemSet.contains(it.getKey()))
            .filter(it -> !Strings.isNullOrEmpty(it.getValue()))
            .collect(Collectors.toList())
            .forEach(entry -> confMap.put(entry.getKey(), EncryptUtil.decrypt(entry.getValue())));

        return confMap;
    }

    private Map<String, String> mergeConf(
        final Map<String, String> defaultConfMap,
        final Map<String, String> connectorConfMap) {

        Preconditions.checkArgument(!CollectionUtils.isEmpty(connectorConfMap));
        Preconditions.checkArgument(!CollectionUtils.isEmpty(defaultConfMap));

        defaultConfMap.putAll(connectorConfMap);
        return defaultConfMap;
    }
}
