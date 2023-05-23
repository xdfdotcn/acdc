package cn.xdf.acdc.devops.service.process.connector;

import java.util.Map;

public interface ConnectorConfigProcessService {
    
    /**
     * Gets the connector configurations, this contains some encrypted configuration items.
     *
     * @param connectorId connectorId
     * @return config map
     */
    Map<String, String> getEncryptConfig(Long connectorId);
    
    /**
     * Gets the connector configurations, all encrypted configuration items are decrypted.
     *
     * @param connectorId connectorId
     * @return config map
     */
    Map<String, String> getDecryptConfig(Long connectorId);
}
