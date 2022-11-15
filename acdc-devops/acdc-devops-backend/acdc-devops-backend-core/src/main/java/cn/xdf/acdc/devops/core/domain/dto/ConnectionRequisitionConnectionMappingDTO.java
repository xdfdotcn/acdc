package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectionRequisitionConnectionMappingDTO {

    private Long id;

    private Long connectionRequisitionId;

    private Long connectionId;

    private Integer connectionVersion;

    public static ConnectionRequisitionConnectionMappingDO toConnectionRequisitionConnectionMappingDO(
            final ConnectionRequisitionConnectionMappingDTO connectionRequisitionConnectionMappingDTO

    ) {
        return ConnectionRequisitionConnectionMappingDO.builder()
                .connection(new ConnectionDO(connectionRequisitionConnectionMappingDTO.getConnectionId()))
                .connectionRequisition(new ConnectionRequisitionDO(connectionRequisitionConnectionMappingDTO.getConnectionRequisitionId()))
                .connectionVersion(connectionRequisitionConnectionMappingDTO.getConnectionVersion())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }


    public static ConnectionRequisitionConnectionMappingDO toConnectionRequisitionConnectionMappingDO(
            final ConnectionRequisitionDTO connectionRequisitionDTO,
            final ConnectionDTO connectionDTO
    ) {
        return ConnectionRequisitionConnectionMappingDO.builder()
                .connection(new ConnectionDO(connectionDTO.getId()))
                .connectionRequisition(new ConnectionRequisitionDO(connectionRequisitionDTO.getId()))
                .connectionVersion(connectionDTO.getVersion())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }

    public static ConnectionRequisitionConnectionMappingDTO toConnectionRequisitionConnectionMappingDTO(
            final ConnectionRequisitionConnectionMappingDO connectionRequisitionConnectionMappingDO

    ) {
        return ConnectionRequisitionConnectionMappingDTO.builder()
                .connectionId(connectionRequisitionConnectionMappingDO.getConnection().getId())
                .connectionRequisitionId(connectionRequisitionConnectionMappingDO.getConnectionRequisition().getId())
                .connectionVersion(connectionRequisitionConnectionMappingDO.getConnectionVersion())
                .build();
    }
}
