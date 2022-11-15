package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.DefaultConnectorConfigurationService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractConnectorConfigProcessServiceImplTest {

    @Mock
    private ConnectorService connectorService;

    @Mock
    private DefaultConnectorConfigurationService defaultConfigService;

    @Mock
    private ConnectorConfigurationService connectorConfigurationService;

    private AbstractConnectorConfigProcessServiceImpl connectConfigProcessService;

    @Before
    public void setup() {
        String encryptPassWord = EncryptUtil.encrypt("acdc");
        List<ConnectorConfigurationDO> connectorConfigurations = Lists.newArrayList(
                new ConnectorConfigurationDO().setName("connection.password").setValue(encryptPassWord),
                new ConnectorConfigurationDO().setName("connection.user").setValue("acdc")
        );

        List<DefaultConnectorConfigurationDO> defaultConnectorConfigurations = Lists.newArrayList(
                new DefaultConnectorConfigurationDO().setName("tasks.max").setValue("1")
        );
        when(connectorService.findById(any()))
                .thenReturn(Optional.of(new ConnectorDO().builder()
                        .connectorClass(new ConnectorClassDO().setId(1L))
                        .build()));
        when(defaultConfigService.findByConnectorClassId(any())).thenReturn(defaultConnectorConfigurations);
        when(connectorConfigurationService.findByConnectorId(any())).thenReturn(connectorConfigurations);

        connectConfigProcessService = new ConnectorConfigProcessServiceImplTest();
        ReflectionTestUtils.setField(connectConfigProcessService, "connectorService", connectorService);
        ReflectionTestUtils.setField(connectConfigProcessService, "defaultConfigService", defaultConfigService);
        ReflectionTestUtils
                .setField(connectConfigProcessService, "connectorConfigurationService", connectorConfigurationService);
    }

    @Test
    public void testGetConfigWithDefaultTest() {
        String encryptPassWord = EncryptUtil.encrypt("acdc");
        Map<String, String> conf = connectConfigProcessService.getConfigWithDefault(1L);
        Assertions.assertThat(conf.get("connection.user")).isEqualTo("acdc");
        Assertions.assertThat(conf.get("connection.password")).isEqualTo(encryptPassWord);
    }

    @Test
    public void testGetDecryptConfigByConfItemSet() {
        Map<String, String> conf = connectConfigProcessService.getDecryptConfigByConfItemSet(1L, Sets.newSet(
                "connection.password"));
        Assertions.assertThat(conf.get("connection.user")).isEqualTo("acdc");
        Assertions.assertThat(conf.get("connection.password")).isEqualTo("acdc");
    }
}
