package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.dto.FieldMappingDTOTest;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class Jdbc2KafkaFieldMappingServiceImplTest {

    private FieldMappingService fieldMappingService;

    @Before
    public void setup() throws SQLException {
        fieldMappingService = new Jdbc2KafkaFieldMappingServiceImpl();
    }

    @Test
    public void testSupportSrcAppTypes() {
        Set<DataSystemType> dataSystemTypes = fieldMappingService.supportSrcAppTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Test
    public void testSupportSinkAppTypes() {
        Set<DataSystemType> dataSystemTypes = fieldMappingService.supportSinkAppTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.KAFKA);
    }

//    @Test
//    public void testSinkConnectDataSystemType() {
//        DataSystemType2 dataSystemType = fieldMappingService.sinkConnectDataSystemType();
//        Assertions.assertThat(DataSystemType2.KAFKA).isEqualTo(dataSystemType);
//    }

    @Test
    public void testNewSequence() {
        FieldMappingDTO fieldMapping = FieldMappingDTOTest.createFieldMapping();
        fieldMapping.setMatchStatus(FieldMappingDTO.IS_MATCH);
        String sequence = fieldMappingService.newSequence(fieldMapping);
        String expect = new StringBuilder()
                .append(FieldMappingDTO.IS_MATCH)
                .append(FieldKeyType.PRI.getSort()).toString();
        Assertions.assertThat(expect).isEqualTo(sequence);
    }

    @Test
    public void testEditSequence() {
        FieldMappingDTO fieldMapping = FieldMappingDTOTest.createFieldMapping();
        String sequence = fieldMappingService.editSequence(fieldMapping);
        String expect = new StringBuilder()
                .append(FieldKeyType.UNI.getSort())
                .append(fieldMapping.getSourceField().getName())
                .toString();
        Assertions.assertThat(expect).isEqualTo(sequence);
    }

    @Test
    public void testDiffingField() {
        List<FieldMappingDTO> fieldMappings = fieldMappingService.diffingField(createSrcFieldMap(), Maps.newHashMap());

        Assertions.assertThat(fieldMappings.size()).isEqualTo(2);

        Assertions.assertThat(fieldMappings.get(0).getMatchStatus()).isEqualTo(FieldMappingDTO.IS_MATCH);
        Assertions.assertThat(fieldMappings.get(0).getSourceField().getName()).isEqualTo("id");
        Assertions.assertThat(fieldMappings.get(0).getSinkField().getName()).isEqualTo("id");

        Assertions.assertThat(fieldMappings.get(1).getMatchStatus()).isEqualTo(FieldMappingDTO.IS_MATCH);
        Assertions.assertThat(fieldMappings.get(1).getSourceField().getName()).isEqualTo("email");
        Assertions.assertThat(fieldMappings.get(1).getSinkField().getName()).isEqualTo("email");
    }

    private Map<String, FieldDTO> createSrcFieldMap() {
        Map<String, FieldDTO> fieldMap = Maps.newHashMap();
        fieldMap.put("id", FieldDTO.builder()
                .name("id")
                .dataType("bigint(20)")
                .allowNull("NO")
                .keyType("PRI")
                .defaultValue("")
                .extra("auto_increment")
                .build()
        );
        fieldMap.put("email", FieldDTO.builder()
                .name("email")
                .dataType("bigint(20)")
                .allowNull("NO")
                .keyType("UNI")
                .defaultValue("")
                .extra("auto_increment")
                .build()
        );
        return fieldMap;
    }
}
