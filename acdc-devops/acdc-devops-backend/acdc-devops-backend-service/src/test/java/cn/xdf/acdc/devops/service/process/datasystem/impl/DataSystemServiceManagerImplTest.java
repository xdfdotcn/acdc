package cn.xdf.acdc.devops.service.process.datasystem.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataSystemServiceManagerImplTest {
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testGetDataSystemMetadataServiceShouldPass() {
        // each data system type must have a data system metadata service
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.MYSQL);
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.TIDB);
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.HIVE);
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.KAFKA);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testGetDataSystemMetadataServiceShouldErrorWhenInputIsOracle() {
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.ORACLE);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testGetDataSystemMetadataServiceShouldErrorWhenInputIsSqlServer() {
        dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.SQLSERVER);
    }
    
    @Test
    public void testGetDataSystemSourceConnectorServiceShouldPass() {
        dataSystemServiceManager.getDataSystemSourceConnectorService(DataSystemType.MYSQL);
        dataSystemServiceManager.getDataSystemSourceConnectorService(DataSystemType.TIDB);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testGetDataSystemSourceConnectorServiceShouldErrorWhenInputIsKafka() {
        // data data system type must have a data system metadata service
        dataSystemServiceManager.getDataSystemSourceConnectorService(DataSystemType.KAFKA);
    }
    
    @Test
    public void testGetDataSystemSinkConnectorServiceShouldPass() {
        // each data system type must have a data system sink connector service
        dataSystemServiceManager.getDataSystemSinkConnectorService(DataSystemType.MYSQL);
        dataSystemServiceManager.getDataSystemSinkConnectorService(DataSystemType.TIDB);
        dataSystemServiceManager.getDataSystemSinkConnectorService(DataSystemType.KAFKA);
        dataSystemServiceManager.getDataSystemSinkConnectorService(DataSystemType.HIVE);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testGetDataSystemSinkConnectorServiceShouldErrorWhenInputIsSqlServer() {
        dataSystemServiceManager.getDataSystemSinkConnectorService(DataSystemType.SQLSERVER);
    }
}
