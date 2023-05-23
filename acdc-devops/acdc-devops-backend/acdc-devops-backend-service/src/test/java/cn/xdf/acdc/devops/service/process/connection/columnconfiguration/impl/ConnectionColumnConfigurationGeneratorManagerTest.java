package cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class ConnectionColumnConfigurationGeneratorManagerTest {
    
    private ConnectionColumnConfigurationGeneratorManager connectionColumnConfigurationGeneratorManager;
    
    @Before
    public void setup() {
        connectionColumnConfigurationGeneratorManager = new ConnectionColumnConfigurationGeneratorManager(
                Lists.newArrayList(
                        new Jdbc2HiveConnectionColumnConfigurationGenerator(),
                        new Jdbc2JdbcConnectionColumnConfigurationGenerator(),
                        new Jdbc2KafkaConnectionColumnConfigurationGenerator())
        );
    }
    
    @Test
    public void testInitGenerators() {
        // TODO
    }
}
