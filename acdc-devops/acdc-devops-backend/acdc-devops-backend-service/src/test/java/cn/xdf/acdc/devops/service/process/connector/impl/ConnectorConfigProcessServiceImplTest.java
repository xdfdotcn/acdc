package cn.xdf.acdc.devops.service.process.connector.impl;

import java.util.Map;
import java.util.Set;

public class ConnectorConfigProcessServiceImplTest extends AbstractConnectorConfigProcessServiceImpl {

    @Override
    public Map<String, String> getEncryptConfig(final Long connectorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getDecryptConfig(final Long connectorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getEncryptConfigItemSet() {
        return null;
    }
}
