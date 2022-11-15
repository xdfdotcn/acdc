package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionVersionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectionDetailDTO {

    private Long id;

    private DataSystemType sourceDataSystemType;

    private Long sourceProjectId;

    private String sourceProjectName;

    private Long sourceDataSystemClusterId;

    private String sourceDataSystemClusterName;

    private Long sourceInstanceId;

    private String sourceInstanceHost;

    private String sourceInstanceVip;

    private String sourceInstancePort;

    private Long sourceDatabaseId;

    private String sourceDatabaseName;

    private Long sourceDataSetId;

    private String sourceDatasetName;

    private DataSystemType sinkDataSystemType;

    private Long sinkProjectId;

    private String sinkProjectName;

    private Long sinkDataSystemClusterId;

    private String sinkDataSystemClusterName;

    private Long sinkInstanceId;

    private String sinkInstanceHost;

    private String sinkInstanceVip;

    private String sinkInstancePort;

    private Long sinkDatabaseId;

    private String sinkDatabaseName;

    private Long sinkDataSetId;

    private String sinkDatasetName;

    private Long userId;

    private String userEmail;

    private String specificConfiguration;

    private Long sourceConnectorId;

    private String sourceConnectorName;

    private Long sinkConnectorId;

    private String sinkConnectorName;

    private Integer version;

    private RequisitionState requisitionState;

    private ConnectionState desiredState;

    private ConnectionState actualState;

    private Instant updateTime;

    private String updateTimeFormat;

    private Instant creationTime;

    private String creationTimeFormat;

    private Boolean deleted;

    private List<FieldMappingDTO> connectionColumnConfigurations;

    public ConnectionDetailDTO(final ConnectionDO connection) {
        this.id = connection.getId();
        this.sourceDataSystemType = connection.getSourceDataSystemType();
        this.sinkDataSystemType = connection.getSinkDataSystemType();
        this.sourceProjectId = connection.getSourceProject().getId();
        this.sinkProjectId = connection.getSinkProject().getId();
        this.sourceDataSetId = connection.getSourceDataSetId();
        this.sinkDataSetId = connection.getSinkDataSetId();
        this.sourceConnectorId = Optional.ofNullable(connection.getSourceConnector()).orElse(new ConnectorDO()).getId();
        this.sinkConnectorId = Optional.ofNullable(connection.getSinkConnector()).orElse(new ConnectorDO()).getId();
        this.sinkInstanceId = connection.getSinkInstanceId();
        this.specificConfiguration = connection.getSpecificConfiguration();
        this.version = connection.getVersion();
        this.deleted = connection.getDeleted();
        this.requisitionState = connection.getRequisitionState();
        this.desiredState = connection.getDesiredState();
        this.actualState = connection.getActualState();
        this.creationTime = connection.getCreationTime();
        this.updateTime = connection.getUpdateTime();
        this.deleted = connection.getDeleted();
    }

    public ConnectionDetailDTO(
            final ConnectionDO connectionDO,
            final Dataset4ConnectionDTO sourceDataset4ConnectionDTO,
            final Dataset4ConnectionDTO sinkDataset4ConnectionDTO,
            final List<FieldMappingDTO> fieldMappingDTOs,
            final UserDTO userDTO
    ) {
        this.id = connectionDO.getId();
        this.sourceDataSystemType = connectionDO.getSourceDataSystemType();
        this.sourceProjectId = sourceDataset4ConnectionDTO.getProjectId();
        this.sourceProjectName = sourceDataset4ConnectionDTO.getProjectName();
        this.sourceDataSystemClusterId = sourceDataset4ConnectionDTO.getClusterId();
        this.sourceDataSystemClusterName = sourceDataset4ConnectionDTO.getClusterName();
        this.sourceInstanceHost = sourceDataset4ConnectionDTO.getInstanceHost();
        this.sourceInstanceId = sourceDataset4ConnectionDTO.getInstanceId();
        this.sourceInstancePort = sourceDataset4ConnectionDTO.getInstancePort();
        this.sourceInstanceVip = sourceDataset4ConnectionDTO.getInstanceVIp();
        this.sourceDatabaseId = sourceDataset4ConnectionDTO.getDatabaseId();
        this.sourceDatabaseName = sourceDataset4ConnectionDTO.getDatabaseName();
        this.sourceDataSetId = sourceDataset4ConnectionDTO.getDataSetId();
        this.sourceDatasetName = sourceDataset4ConnectionDTO.getDatasetName();
        this.sinkDataSystemType = connectionDO.getSinkDataSystemType();
        this.sinkProjectId = sinkDataset4ConnectionDTO.getProjectId();
        this.sinkProjectName = sinkDataset4ConnectionDTO.getProjectName();
        this.sinkDataSystemClusterId = sinkDataset4ConnectionDTO.getClusterId();
        this.sinkDataSystemClusterName = sinkDataset4ConnectionDTO.getClusterName();
        this.sinkInstanceId = sinkDataset4ConnectionDTO.getInstanceId();
        this.sinkInstanceHost = sinkDataset4ConnectionDTO.getInstanceHost();
        this.sinkInstancePort = sinkDataset4ConnectionDTO.getInstancePort();
        this.sinkInstanceVip = sinkDataset4ConnectionDTO.getInstanceVIp();
        this.sinkDatabaseId = sinkDataset4ConnectionDTO.getDatabaseId();
        this.sinkDatabaseName = sinkDataset4ConnectionDTO.getDatabaseName();
        this.sinkDataSetId = sinkDataset4ConnectionDTO.getDataSetId();
        this.sinkDatasetName = sinkDataset4ConnectionDTO.getDatasetName();
        this.userEmail = userDTO.getEmail();
        this.userId = userDTO.getId();
        this.specificConfiguration = sinkDataset4ConnectionDTO.getSpecificConfiguration();
        this.sourceConnectorId = Optional.ofNullable(connectionDO.getSourceConnector()).orElse(new ConnectorDO()).getId();
        this.sinkConnectorId = Optional.ofNullable(connectionDO.getSinkConnector()).orElse(new ConnectorDO()).getId();
        this.version = connectionDO.getVersion();
        this.requisitionState = connectionDO.getRequisitionState();
        this.desiredState = connectionDO.getDesiredState();
        this.actualState = connectionDO.getActualState();
        this.updateTime = connectionDO.getUpdateTime();
        this.updateTimeFormat = DateUtil.formatToString(connectionDO.getUpdateTime());
        this.creationTime = connectionDO.getCreationTime();
        this.creationTimeFormat = DateUtil.formatToString(connectionDO.getCreationTime());
        this.deleted = connectionDO.getDeleted();
        this.connectionColumnConfigurations = fieldMappingDTOs;
    }

    public ConnectionDetailDTO(
            final ConnectionDO connectionDO,
            final Dataset4ConnectionDTO sourceDataset4ConnectionDTO,
            final Dataset4ConnectionDTO sinkDataset4ConnectionDTO,
            final List<FieldMappingDTO> fieldMappingDTOs,
            final UserDTO userDTO,
            final SourceConnectorInfoDTO sourceConnectorInfoDTO,
            final SinkConnectorInfoDTO sinkConnectorInfoDTO
    ) {
        this(connectionDO, sourceDataset4ConnectionDTO, sinkDataset4ConnectionDTO, fieldMappingDTOs, userDTO);

        this.setSourceConnectorName(sourceConnectorInfoDTO.getName());
        this.setSinkConnectorName(sinkConnectorInfoDTO.getName());
    }

    public ConnectionDO toConnectionDO() {
        ConnectionVersionDO version = ConnectionVersionDO.initVersion();
        return ConnectionDO.builder()
                .sourceDataSystemType(this.getSourceDataSystemType())
                .sinkDataSystemType(this.getSinkDataSystemType())
                .sourceProject(new ProjectDO().setId(this.getSourceProjectId()))
                .sinkProject(new ProjectDO().setId(this.getSinkProjectId()))
                .sourceDataSetId(this.getSourceDataSetId())
                .sinkDataSetId(this.getSinkDataSetId())
                .sinkInstanceId(this.getSinkInstanceId())
                .specificConfiguration(this.getSpecificConfiguration())
                .user(new UserDO().setId(this.getUserId()))
                .creationTime(DateUtil.getInstantWithDefault(this.getCreationTime(), () -> Instant.now()))
                .updateTime(Instant.now())
                .deleted(Boolean.FALSE)
                .requisitionState(RequisitionState.APPROVING)
                .actualState(ConnectionState.STOPPED)
                .desiredState(ConnectionState.STOPPED)
                .version(version.incrementVersion())
                .build();
    }

    public ConnectorCreationDTO toConnectorCreationDTO() {
        return ConnectorCreationDTO.builder()
                .sourceDataSystemType(this.getSourceDataSystemType())
                .sinkDataSystemType(this.getSinkDataSystemType())
                .sourceDataset(getSourceDataSet())
                .sinkDataset(getSinkDataSet())
                .fieldMappings(this.getConnectionColumnConfigurations())
                .build();
    }

    public DataSetDTO getSourceDataSet() {
        return DataSetDTO.builder()
                .dataSystemType(this.getSourceDataSystemType())
                .projectId(this.getSourceProjectId())
                .dataSetId(this.getSourceDataSetId())
                .build();
    }

    public DataSetDTO getSinkDataSet() {
        return DataSetDTO.builder()
                .dataSystemType(this.getSinkDataSystemType())
                .projectId(this.getSinkProjectId())
                .dataSetId(this.getSinkDataSetId())
                .instanceId(this.getSinkInstanceId())
                .specificConfiguration(this.getSpecificConfiguration())
                .build();
    }

    // TODO: 转移到其他 DTO 中
    @Data
    public static class Tuple {

        private ConnectionDO connection;

        private List<ConnectionColumnConfigurationDO> columnConfigs;

        public Tuple(final ConnectionDO connection, final List<ConnectionColumnConfigurationDO> columnConfigs) {
            this.connection = connection;
            this.columnConfigs = columnConfigs;
        }
    }
}
