package cn.xdf.acdc.devops.service.process.utils;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.util.SourceConnectorConfigurationUtil;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class SourceConnectorConfigurationUtilTest {
    
    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testGenerateConnectorMessageKeyColumnsConfigurationValueShouldUsePkForMessageKeyColumnWhenPKExists() {
        List<DataFieldDefinition> table1DataFieldDefinitions = new ArrayList<>();
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_1", "bigint", Sets.newHashSet("PRIMARY")));
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_2", "varchar(32)", Sets.newHashSet("unique_index_1")));
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_3", "varchar(128)", Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_4", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));
        
        List<DataFieldDefinition> table2DataFieldDefinitions = new ArrayList<>();
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_1", "bigint", Sets.newHashSet("PRIMARY")));
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_2", "varchar(32)", Sets.newHashSet("unique_index_1")));
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_3", "varchar(128)", Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_4", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));
    
        List<DataCollectionDefinition> dataCollectionDefinitions = new ArrayList<>();
        dataCollectionDefinitions.add(new DataCollectionDefinition("table_1", table1DataFieldDefinitions));
        dataCollectionDefinitions.add(new DataCollectionDefinition("table_2", table2DataFieldDefinitions));
        
        DataSystemResourceDTO database = new DataSystemResourceDTO().setId(1L).setName("database");
        
        String messageKey = SourceConnectorConfigurationUtil.generateConnectorMessageKeyColumnsConfigurationValue(database, dataCollectionDefinitions, "PRIMARY");
        
        Assertions.assertThat(messageKey).isEqualTo("database.table_1:table_1_column_1;database.table_2:table_2_column_1;");
    }
    
    @Test
    public void testGenerateConnectorMessageKeyColumnsConfigurationValueShouldUseUniqueIndexForMessageKeyColumnWhenPKNotExists() {
        List<DataFieldDefinition> table1DataFieldDefinitions = new ArrayList<>();
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_1", "varchar(32)", Sets.newHashSet("unique_index_1")));
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_2", "varchar(128)", Sets.newHashSet("unique_index_2")));
        
        List<DataFieldDefinition> table2DataFieldDefinitions = new ArrayList<>();
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_1", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));
        table2DataFieldDefinitions.add(new DataFieldDefinition("table_2_column_2", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));
    
        List<DataCollectionDefinition> dataCollectionDefinitions = new ArrayList<>();
        dataCollectionDefinitions.add(new DataCollectionDefinition("table_1", table1DataFieldDefinitions));
        dataCollectionDefinitions.add(new DataCollectionDefinition("table_2", table2DataFieldDefinitions));
        
        DataSystemResourceDTO database = new DataSystemResourceDTO().setId(1L).setName("database");
        
        String messageKey = SourceConnectorConfigurationUtil.generateConnectorMessageKeyColumnsConfigurationValue(database, dataCollectionDefinitions, "PRIMARY");
        
        Assertions.assertThat(messageKey).isEqualTo("database.table_1:table_1_column_1;database.table_2:table_2_column_1,table_2_column_2;");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGenerateConnectorMessageKeyColumnsConfigurationValueShouldErrorWhenUniqueIndexNotExists() {
        List<DataCollectionDefinition> dataCollectionDefinitions = new ArrayList<>();
        
        List<DataFieldDefinition> table1DataFieldDefinitions = new ArrayList<>();
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_1", "varchar(32)", Collections.emptySet()));
        table1DataFieldDefinitions.add(new DataFieldDefinition("table_1_column_2", "varchar(128)", Collections.emptySet()));
        
        dataCollectionDefinitions.add(new DataCollectionDefinition("table_1", table1DataFieldDefinitions));
        
        DataSystemResourceDTO database = new DataSystemResourceDTO().setId(1L).setName("database");
        
        SourceConnectorConfigurationUtil.generateConnectorMessageKeyColumnsConfigurationValue(database, dataCollectionDefinitions, "PRIMARY");
    }
}
