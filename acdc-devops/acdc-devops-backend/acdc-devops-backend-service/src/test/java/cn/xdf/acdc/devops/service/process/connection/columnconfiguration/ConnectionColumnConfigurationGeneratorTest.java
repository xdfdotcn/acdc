package cn.xdf.acdc.devops.service.process.connection.columnconfiguration;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.util.StringUtil;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl.Jdbc2HiveConnectionColumnConfigurationGenerator;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
public class ConnectionColumnConfigurationGeneratorTest {
    
    private ConnectionColumnConfigurationGenerator connectionColumnConfigurationGenerator;
    
    @Before
    public void setup() {
        connectionColumnConfigurationGenerator = new Jdbc2HiveConnectionColumnConfigurationGenerator();
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnAcdcMetaFieldOrderValue() {
        for (String metaFiled : ConnectionColumnConfigurationConstant.META_FIELD_SET) {
            DataFieldDefinition definition = new DataFieldDefinition(metaFiled, "string", Sets.newHashSet());
            
            int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
            
            int expectOrderValue = ConnectionColumnConfigurationGenerator.ACDC_META_FIELD_ORDER_VALUE;
            Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
        }
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnNoneUniqueIndexOrderValue() {
        DataFieldDefinition definition = new DataFieldDefinition("id", "int", Sets.newHashSet());
        
        int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
        
        int expectOrderValue = ConnectionColumnConfigurationGenerator.NORMAL_FIELD_INDEX_ORDER_VALUE;
        
        Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnPrimaryIndexOrderValue() {
        DataFieldDefinition definition = new DataFieldDefinition("id", "int", Sets.newHashSet("PRIMARY", "test_index1"));
        
        int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
        
        int expectOrderValue = ConnectionColumnConfigurationGenerator.PRIMARY_INDEX_ORDER_VALUE;
        
        Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnUniqueIndexOrderValue() {
        DataFieldDefinition definition = new DataFieldDefinition("id", "int", Sets.newHashSet("test_index1", "test_index2"));
        
        int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
        
        int expectOrderValue = ConnectionColumnConfigurationGenerator.UNIQUE_INDEX_ORDER_VALUE;
        
        Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnNormalFieldOrderValue() {
        DataFieldDefinition definition = new DataFieldDefinition("id", "int", Sets.newHashSet());
        
        int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
        
        int expectOrderValue = ConnectionColumnConfigurationGenerator.NORMAL_FIELD_INDEX_ORDER_VALUE;
        
        Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
    }
    
    @Test
    public void testCalculateFieldDefinitionOrderValueShouldReturnEmptyFieldOrderValue() {
        DataFieldDefinition definition = new DataFieldDefinition("", "int", Sets.newHashSet());
        
        int orderValue = connectionColumnConfigurationGenerator.calculateFieldDefinitionOrderValue(definition);
        
        int expectOrderValue = ConnectionColumnConfigurationGenerator.EMPTY_FIELD_INDEX_ORDER_VALUE;
        
        Assertions.assertThat(orderValue).isEqualTo(expectOrderValue);
    }
    
    @Test
    public void testCalculateColumnMatchingRateShouldReturnMatchedValue() {
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("id");
        connectionColumnConfiguration.setSourceColumnType("bigint(13)");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames(Sets.newHashSet("test_index"));
        connectionColumnConfiguration.setSinkColumnName("iD");
        connectionColumnConfiguration.setSinkColumnType("bigint(14)");
        
        int matchingRate = connectionColumnConfigurationGenerator.calculateColumnMatchingRate(connectionColumnConfiguration);
        
        int expectMatchingRate = ConnectionColumnConfigurationGenerator.IS_MATCH;
        Assertions.assertThat(matchingRate).isEqualTo(expectMatchingRate);
    }
    
    @Test
    public void testCalculateColumnMatchingRateShouldReturnNotMatchedValue() {
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("id");
        connectionColumnConfiguration.setSourceColumnType("bigint(13)");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames(Sets.newHashSet("test_index"));
        connectionColumnConfiguration.setSinkColumnName("myid");
        connectionColumnConfiguration.setSinkColumnType("bigint(14)");
        
        int matchingRate = connectionColumnConfigurationGenerator.calculateColumnMatchingRate(connectionColumnConfiguration);
        int expectMatchingRate = ConnectionColumnConfigurationGenerator.NOT_MATCH;
        Assertions.assertThat(matchingRate).isEqualTo(expectMatchingRate);
    }
    
    @Test
    public void testCalculateColumnMatchingRateShouldReturnNotMatchedValueWhenExistNullField() {
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("id");
        connectionColumnConfiguration.setSourceColumnType("bigint(13)");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames(Sets.newHashSet("test_index"));
        
        int matchingRate = connectionColumnConfigurationGenerator.calculateColumnMatchingRate(connectionColumnConfiguration);
        
        int expectMatchingRate = ConnectionColumnConfigurationGenerator.NOT_MATCH;
        Assertions.assertThat(matchingRate).isEqualTo(expectMatchingRate);
    }
    
    @Test
    public void testGenerateSequenceWhenNew() {
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("source_id");
        connectionColumnConfiguration.setSourceColumnType("bigint(13)");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"));
        connectionColumnConfiguration.setSinkColumnName("sink_id");
        connectionColumnConfiguration.setSinkColumnType("bigint(13)");
        connectionColumnConfiguration.setSinkColumnUniqueIndexNames(Sets.newHashSet("test_index"));
        
        String sequence = connectionColumnConfigurationGenerator.generateSequenceWhenNew(connectionColumnConfiguration);
        
        String expectSequence = new StringBuffer()
                .append(ConnectionColumnConfigurationGenerator.NOT_MATCH)
                .append(ConnectionColumnConfigurationGenerator.PRIMARY_INDEX_ORDER_VALUE)
                .append("source_id").toString();
        
        Assertions.assertThat(sequence).isEqualTo(expectSequence);
    }
    
    @Test
    public void testGenerateSequenceWhenEdit() {
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("source_id");
        connectionColumnConfiguration.setSourceColumnType("bigint(13)");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames(Sets.newHashSet("test_index"));
        connectionColumnConfiguration.setSinkColumnName("sink_id");
        connectionColumnConfiguration.setSinkColumnType("bigint(13)");
        
        String sequence = connectionColumnConfigurationGenerator.generateSequenceWhenEdit(connectionColumnConfiguration);
        
        String expectSequence = new StringBuffer()
                .append(ConnectionColumnConfigurationGenerator.UNIQUE_INDEX_ORDER_VALUE)
                .append("source_id").toString();
        
        Assertions.assertThat(sequence).isEqualTo(expectSequence);
    }
    
    @Test
    public void testGenerateColumnConfigurationShouldReturnEmptyWhenSinkDataCollectionIsEmpty() {
        List<DataFieldDefinition> dataFieldDefinitions = Lists.newArrayList(new DataFieldDefinition("f1", "int", Collections.EMPTY_SET));
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", dataFieldDefinitions);
        DataCollectionDefinition sinkDataCollectionDefinition = new DataCollectionDefinition("sink", Collections.EMPTY_LIST);
        
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = connectionColumnConfigurationGenerator
                .generateColumnConfiguration(sourceDataCollectionDefinition, sinkDataCollectionDefinition);
        
        Assertions.assertThat(connectionColumnConfigurations).isEmpty();
    }
    
    @Test
    public void testGenerateColumnConfigurationShouldPassWhenSourceDataCollectionIsEmpty() {
        List<DataFieldDefinition> dataFieldDefinitions = Lists.newArrayList(new DataFieldDefinition("f1", "int", Collections.EMPTY_SET));
        DataCollectionDefinition sinkDataCollectionDefinition = new DataCollectionDefinition("sink", dataFieldDefinitions);
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", Collections.EMPTY_LIST);
        
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = connectionColumnConfigurationGenerator
                .generateColumnConfiguration(sourceDataCollectionDefinition, sinkDataCollectionDefinition);
        
        Assertions.assertThat(connectionColumnConfigurations).isNotEmpty();
    }
    
    @Test
    public void testBuildColumnConfigurationShouldNotMatch() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f2", "int", Sets.newHashSet("test_index1", "test_index2"));
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY"))
        );
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("sink", sourceDataFieldDefinitions);
        
        ConnectionColumnConfigurationDTO configuration = connectionColumnConfigurationGenerator
                .buildColumnConfiguration(sinkDataFieldDefinition, sourceDataCollectionDefinition);
        
        Assertions.assertThat(configuration.getSourceColumnName()).isNull();
        Assertions.assertThat(configuration.getSourceColumnType()).isBlank();
        Assertions.assertThat(configuration.getSourceColumnUniqueIndexNames()).isEmpty();
        
        Assertions.assertThat(configuration.getSinkColumnName()).isEqualTo("f2");
        Assertions.assertThat(configuration.getSinkColumnType()).isEqualTo("int");
        Assertions.assertThat(configuration.getSinkColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("test_index1", "test_index2"));
    }
    
    @Test
    public void testBuildColumnConfigurationShouldMatch() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("F1", "int", Sets.newHashSet("test_index1", "test_index2"));
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "bigint", Sets.newHashSet("PRIMARY"))
        );
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("sink", sourceDataFieldDefinitions);
        
        ConnectionColumnConfigurationDTO configuration = connectionColumnConfigurationGenerator
                .buildColumnConfiguration(sinkDataFieldDefinition, sourceDataCollectionDefinition);
        
        Assertions.assertThat(configuration.getSourceColumnName()).isEqualTo("f1");
        Assertions.assertThat(configuration.getSourceColumnType()).isEqualTo("bigint");
        Assertions.assertThat(configuration.getSourceColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("PRIMARY"));
        
        Assertions.assertThat(configuration.getSinkColumnName()).isEqualTo("F1");
        Assertions.assertThat(configuration.getSinkColumnType()).isEqualTo("int");
        Assertions.assertThat(configuration.getSinkColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("test_index1", "test_index2"));
    }
    
    @Test
    public void testMaybeSourceColumnMissingShouldReturnTrueWhenSourceFiledNotInSourceDataCollectionDefinition() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(new DataFieldDefinition("f1", "int", Collections.EMPTY_SET));
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("f2");
        
        boolean result = connectionColumnConfigurationGenerator.maybeSourceColumnMissing(sourceDataCollectionDefinition, connectionColumnConfiguration);
        
        Assertions.assertThat(result).isTrue();
    }
    
    @Test
    public void testMaybeSourceColumnMissingShouldReturnFalseWhenSourceFiledIsMetaField() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(new DataFieldDefinition("f1", "int", Collections.EMPTY_SET));
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("");
        
        boolean result = connectionColumnConfigurationGenerator.maybeSourceColumnMissing(sourceDataCollectionDefinition, connectionColumnConfiguration);
        
        Assertions.assertThat(result).isFalse();
    }
    
    @Test
    public void testMaybeSourceColumnMissingShouldReturnFalseWhenSourceFiledInSourceDataCollectionDefinition() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(new DataFieldDefinition("f1", "int", Collections.EMPTY_SET));
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
        connectionColumnConfiguration.setSourceColumnName("F1");
        
        boolean result = connectionColumnConfigurationGenerator.maybeSourceColumnMissing(sourceDataCollectionDefinition, connectionColumnConfiguration);
        
        Assertions.assertThat(result).isFalse();
    }
    
    @Test
    public void testIsMetaFiled() {
        for (String field : ConnectionColumnConfigurationConstant.META_FIELD_SET) {
            boolean result = connectionColumnConfigurationGenerator.isMetaFiled(field);
            Assertions.assertThat(result).isTrue();
        }
        
        Assertions.assertThat(connectionColumnConfigurationGenerator.isMetaFiled(null)).isFalse();
    }
    
    //===============================================
    // case: no any change for source and sink
    // case: add filed for sink
    // case: delete filed for sink
    // case: field type and index change for sink
    // case: add filed for source
    // case: delete filed for source
    // case: field type and index change for source
    // case: ignore source meta filed check
    //===============================================
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationShouldDoNothingWhenFiledNotChange() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY"));
        
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f2", "int", Sets.newHashSet("PRIMARY"))
        );
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f1", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("F2")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("F1")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, "f2", "int", "PRIMARY");
        verifyColumnConfigurationSinkField(columnConfiguration, "f1", "int", "PRIMARY");
    }
    
    // add field for sink
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenAddSinkFiled() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f2", "int", Sets.newHashSet());
        
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY"))
        );
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f1", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("f1")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("f1")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, null, null, null);
        verifyColumnConfigurationSinkField(columnConfiguration, "f2", "int", null);
    }
    
    // delete field for sink
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenDeleteSinkFiled() {
    }
    
    // field type and index change for sink
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenDeleteSinkFiledTypeAndIndexChanged() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f2", "bigint", Sets.newHashSet("idx1"));
        
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY"))
        );
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f2", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("f1")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("f2")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, "f1", "int", "PRIMARY");
        verifyColumnConfigurationSinkField(columnConfiguration, "f2", "bigint", "idx1");
    }
    
    // add field for source
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenAddSourceFiled() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("f2", "int", Sets.newHashSet("idx1"))
        );
        
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f3", "bigint", Sets.newHashSet("PRIMARY"));
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f3", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("f1")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("f3")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, "f1", "int", "PRIMARY");
        verifyColumnConfigurationSinkField(columnConfiguration, "f3", "bigint", "PRIMARY");
    }
    
    // delete field for source
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenDeleteSourceFiled() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f2", "int", Sets.newHashSet("idx1"));
        
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList();
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f1", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("f1")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("f1")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, null, null, null);
        verifyColumnConfigurationSinkField(columnConfiguration, "f2", "int", "idx1");
    }
    
    // the current column configuration source filed is meta filed, ignore source filed changed
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenConfigurationSourceFieldIsMetaFiled() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("f2", "int", Sets.newHashSet("idx1"))
        );
        
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f3", "bigint", Sets.newHashSet("PRIMARY"));
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f3", new ConnectionColumnConfigurationDTO()
                .setSourceColumnType("any")
                .setSinkColumnName("f3")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, null, "any", null);
        verifyColumnConfigurationSinkField(columnConfiguration, "f3", "bigint", "PRIMARY");
    }
    
    // field type and index change for source
    @Test
    public void testBuildColumnConfigurationWithColumnConfigurationWhenDeleteSourceFiledTypeAndIndexChanged() {
        DataFieldDefinition sinkDataFieldDefinition = new DataFieldDefinition("f2", "bigint", Sets.newHashSet("idx1"));
        
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "bigint", Sets.newHashSet("source_idx1"))
        );
        
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping = new HashMap<>();
        
        sinkColumnNameAndColumnConfigurationMapping.put("f2", new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("f1")
                .setSourceColumnType("int")
                .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                .setSinkColumnName("f2")
                .setSinkColumnType("int")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
        );
        
        ConnectionColumnConfigurationDTO columnConfiguration = connectionColumnConfigurationGenerator.buildColumnConfiguration(
                sinkDataFieldDefinition,
                sourceDataCollectionDefinition,
                sinkColumnNameAndColumnConfigurationMapping
        );
        
        verifyColumnConfigurationSourceField(columnConfiguration, "f1", "bigint", "source_idx1");
        verifyColumnConfigurationSinkField(columnConfiguration, "f2", "bigint", "idx1");
    }
    
    @Test
    public void testGenerateColumnConfiguration() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "int", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("f2", "int", Sets.newHashSet("src_idx1"))
        );
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        List<DataFieldDefinition> sinkDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("f1", "bigint", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("f2", "bigint", Sets.newHashSet("sink_idx1"))
        );
        DataCollectionDefinition sinkDataCollectionDefinition = new DataCollectionDefinition("sink", sinkDataFieldDefinitions);
        
        List<ConnectionColumnConfigurationDTO> columnConfigurations = connectionColumnConfigurationGenerator.generateColumnConfiguration(
                sourceDataCollectionDefinition,
                sinkDataCollectionDefinition
        );
        
        verifyColumnConfigurationSourceField(columnConfigurations.get(0), "f1", "int", "PRIMARY");
        verifyColumnConfigurationSinkField(columnConfigurations.get(0), "f1", "bigint", "PRIMARY");
        verifyColumnConfigurationSourceField(columnConfigurations.get(1), "f2", "int", "src_idx1");
        verifyColumnConfigurationSinkField(columnConfigurations.get(1), "f2", "bigint", "sink_idx1");
        
        Assertions.assertThat(columnConfigurations.get(0).getId()).isEqualTo(1L);
        Assertions.assertThat(columnConfigurations.get(1).getId()).isEqualTo(2L);
    }
    
    @Test
    public void testGenerateColumnConfigurationWithColumnConfiguration() {
        List<DataFieldDefinition> sourceDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("q1", "int", Sets.newHashSet("PRIMARY"))
        );
        DataCollectionDefinition sourceDataCollectionDefinition = new DataCollectionDefinition("source", sourceDataFieldDefinitions);
        
        List<DataFieldDefinition> sinkDataFieldDefinitions = Lists.newArrayList(
                new DataFieldDefinition("q1", "bigint", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("q2", "bigint", Sets.newHashSet("PRIMARY"))
        );
        DataCollectionDefinition sinkDataCollectionDefinition = new DataCollectionDefinition("sink", sinkDataFieldDefinitions);
        
        List<ConnectionColumnConfigurationDTO> alreadyExistConnectionColumnConfigurations = Lists.newArrayList(
                new ConnectionColumnConfigurationDTO()
                        .setSourceColumnName("f1")
                        .setSourceColumnType("int")
                        .setSourceColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"))
                        .setSinkColumnName("f1")
                        .setSinkColumnType("int")
                        .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY")),
                new ConnectionColumnConfigurationDTO()
                        .setSourceColumnName("f2")
                        .setSourceColumnType("int")
                        .setSourceColumnUniqueIndexNames(Sets.newHashSet("src_idx1"))
                        .setSinkColumnName("f2")
                        .setSinkColumnType("int")
                        .setSinkColumnUniqueIndexNames(Sets.newHashSet("sink_idx1"))
        );
        
        List<ConnectionColumnConfigurationDTO> columnConfigurations = connectionColumnConfigurationGenerator.generateColumnConfiguration(
                sourceDataCollectionDefinition,
                sinkDataCollectionDefinition,
                alreadyExistConnectionColumnConfigurations
        );
        
        verifyColumnConfigurationSourceField(columnConfigurations.get(0), null, null, null);
        verifyColumnConfigurationSinkField(columnConfigurations.get(0), "q1", "bigint", "PRIMARY");
        
        Assertions.assertThat(columnConfigurations.get(0).getId()).isEqualTo(1L);
    }
    
    private void verifyColumnConfigurationSourceField(
            final ConnectionColumnConfigurationDTO configuration,
            final String expectName,
            final String expectType,
            final String expectUniqueIndexName
    ) {
        Assertions.assertThat(configuration.getSourceColumnName()).isEqualTo(expectName);
        Assertions.assertThat(configuration.getSourceColumnType()).isEqualTo(expectType);
        
        Set<String> expectUniqueIndexNameSet = StringUtil.convertStringToSetWithSeparator(expectUniqueIndexName, Symbol.COMMA);
        Set<String> actualUniqueIndexNameSet = configuration.getSourceColumnUniqueIndexNames();
        Assertions.assertThat(actualUniqueIndexNameSet).isEqualTo(expectUniqueIndexNameSet);
    }
    
    private void verifyColumnConfigurationSinkField(
            final ConnectionColumnConfigurationDTO configuration,
            final String expectName,
            final String expectType,
            final String expectUniqueIndexName
    ) {
        Assertions.assertThat(configuration.getSinkColumnName()).isEqualTo(expectName);
        Assertions.assertThat(configuration.getSinkColumnType()).isEqualTo(expectType);
        
        Set<String> expectUniqueIndexNameSet = StringUtil.convertStringToSetWithSeparator(expectUniqueIndexName, Symbol.COMMA);
        Set<String> actualUniqueIndexNameSet = configuration.getSinkColumnUniqueIndexNames();
        Assertions.assertThat(actualUniqueIndexNameSet).isEqualTo(expectUniqueIndexNameSet);
    }
}
