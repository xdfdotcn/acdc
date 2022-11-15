package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.dto.FieldMappingDTOTest;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class Jdbc2JdbcFieldMappingServiceImplTest {

    private FieldMappingService fieldMappingService;

    @Before
    public void setup() throws SQLException {
        fieldMappingService = new Jdbc2JdbcFieldMappingServiceImpl();
    }

    @Test
    public void testSupportSrcAppTypes() {
        Set<DataSystemType> dataSystemTypes = fieldMappingService.supportSrcAppTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Test
    public void testSupportSinkAppTypes() {
        Set<DataSystemType> dataSystemTypes = fieldMappingService.supportSinkAppTypes();
        Assertions.assertThat(dataSystemTypes).contains(DataSystemType.MYSQL, DataSystemType.TIDB);
    }
//
//    @Test
//    public void testSinkConnectDataSystemType() {
//        DataSystemType2 dataSystemType = fieldMappingService.sinkConnectDataSystemType();
//        Assertions.assertThat(DataSystemType2.JDBC).isEqualTo(dataSystemType);
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
                .append(FieldKeyType.PRI.getSort())
                .append(fieldMapping.getSinkField().getName())
                .toString();
        Assertions.assertThat(expect).isEqualTo(sequence);
    }
}
