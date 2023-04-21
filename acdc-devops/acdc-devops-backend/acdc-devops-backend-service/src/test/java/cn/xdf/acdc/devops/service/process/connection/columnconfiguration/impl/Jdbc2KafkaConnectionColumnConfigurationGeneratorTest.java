package cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationGenerator;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class Jdbc2KafkaConnectionColumnConfigurationGeneratorTest {

    private ConnectionColumnConfigurationGenerator connectionColumnConfigurationGenerator;

    @Before
    public void setup() {
        connectionColumnConfigurationGenerator = new Jdbc2KafkaConnectionColumnConfigurationGenerator();
    }

    @Test
    public void testSupportSrcAppTypes() {
        Set<DataSystemType> dataSystemTypes = connectionColumnConfigurationGenerator.supportedSourceDataSystemTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Test
    public void testSupportSinkAppTypes() {
        Set<DataSystemType> dataSystemTypes = connectionColumnConfigurationGenerator.supportedSinkDataSystemTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.KAFKA);
    }
}