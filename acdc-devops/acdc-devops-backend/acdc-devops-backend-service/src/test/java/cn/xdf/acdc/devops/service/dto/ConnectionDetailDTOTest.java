package cn.xdf.acdc.devops.service.dto;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.List;

public class ConnectionDetailDTOTest {

    private List<FieldMappingDTO> fieldMappings = FieldMappingDTOTest.createFieldMappings();

    private ConnectionDetailDTO connection = ConnectionDetailDTO.builder()
            .sourceDataSystemType(DataSystemType.MYSQL)
            .sinkDataSystemType(DataSystemType.MYSQL)
            .sourceProjectId(1L)
            .sinkProjectId(2L)
            .sourceDataSetId(3L)
            .sinkDataSetId(4L)
            .sinkInstanceId(5L)
            .specificConfiguration("{}")
            .userId(1L)
            .connectionColumnConfigurations(fieldMappings)
            .build();

    // Mapper instances are fully thread-safe provided
//    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testToConnectorCreationDTO() {
        ConnectorCreationDTO expect = ConnectorCreationDTO.builder()
                .sourceDataset(DataSetDTO.builder()
                        .dataSetId(3L)
                        .projectId(1L)
                        .dataSystemType(DataSystemType.MYSQL)
                        .build())
                .sinkDataset(DataSetDTO.builder()
                        .projectId(2L)
                        .dataSystemType(DataSystemType.MYSQL)
                        .dataSetId(4L)
                        .instanceId(5L)
                        .specificConfiguration("{}").build())
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)
                .fieldMappings(fieldMappings).build();
        ConnectorCreationDTO creation = connection.toConnectorCreationDTO();
        Assertions.assertThat(expect).isEqualTo(creation);
    }

    @Test
    public void testToConnectionDO4Create() {
        ConnectionDO expect = ConnectionDO.builder()
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)
                .sourceProject(new ProjectDO().setId(1L))
                .sinkProject(new ProjectDO().setId(2L))
                .sourceDataSetId(3L)
                .sinkDataSetId(4L)
                .sinkInstanceId(5L)
                .specificConfiguration("{}")
                .user(new UserDO().setId(1L))
                .deleted(false)
                .requisitionState(RequisitionState.APPROVING)
                .actualState(ConnectionState.STOPPED)
                .desiredState(ConnectionState.STOPPED)
                .version(1)
                .build();
        ConnectionDO connectionDO = connection.toConnectionDO();
        connectionDO.setCreationTime(null);
        connectionDO.setUpdateTime(null);
        Assertions.assertThat(connectionDO).isEqualTo(expect);
    }

    @Test
    public void testToConnectionDTO() {
        ConnectionDetailDTO expect = ConnectionDetailDTO.builder()
                .id(99L)
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)

                .sourceProjectId(1L)
                .sinkProjectId(2L)

                .sourceDataSetId(3L)
                .sinkDataSetId(4L)
                .sinkInstanceId(5L)

                .sourceConnectorId(6L)
                .sinkConnectorId(7L)
                .specificConfiguration("{}")

                .version(2)
                .requisitionState(RequisitionState.APPROVING)
                .desiredState(ConnectionState.STOPPED)
                .actualState(ConnectionState.STOPPED)

                .deleted(Boolean.FALSE)
                .build();

        ConnectionDO connectionDO = ConnectionDO.builder()
                .id(99L)
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)

                .sourceProject(new ProjectDO().setId(1L))
                .sinkProject(new ProjectDO().setId(2L))

                .sourceDataSetId(3L)
                .sinkDataSetId(4L)
                .sinkInstanceId(5L)

                .sourceConnector(new ConnectorDO().setId(6L))
                .sinkConnector(new ConnectorDO().setId(7L))

                .specificConfiguration("{}")

                .version(2)
                .requisitionState(RequisitionState.APPROVING)
                .desiredState(ConnectionState.STOPPED)
                .actualState(ConnectionState.STOPPED).build();

        ConnectionDetailDTO actual = new ConnectionDetailDTO(connectionDO);
        Assertions.assertThat(expect).isEqualTo(actual);
    }
}
