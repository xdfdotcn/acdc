package cn.xdf.acdc.devops.core.domain.dto;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.util.DateUtil;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectionInfoDTO {

    private static final String PATH_SEPARATOR = "/";

    // basic info
    private Long connectionId;

    private Long sinkConnectorId;

    private Long sourceConnectorId;

    private DataSystemType sourceDataSystemType;

    private DataSystemType sinkDataSystemType;

    private ConnectionState desiredState;

    private ConnectionState actualState;

    private RequisitionState requisitionState;

    private String creationTimeFormat;

    private String updateTimeFormat;

    private String sourcePath;

    private String sinkPath;


    // id info
    private Long sourceProjectId;

    private Long sinkProjectId;

    private Long sourceDatasetClusterId;

    private Long sinkDatasetClusterId;

    private Long sourceDatasetDatabaseId;

    private Long sinkDatasetDatabaseId;

    private Long sourceDatasetId;

    private Long sinkDatasetId;

    private Long sinkDatasetInstanceId;

    // name info
    private String sourceProjectName;

    private String sinkProjectName;

    private String sourceDatasetClusterName;

    private String sinkDatasetClusterName;

    private String sourceDatasetDatabaseName;

    private String sinkDatasetDatabaseName;

    private String sourceDatasetName;

    private String sinkDatasetName;

    private String sinkDatasetInstanceName;

    public static ConnectionInfoDTO toConnectionInfoDTO(Map<String, Object> resultMap) {

        return ConnectionInfoDTO.builder()
                .connectionId(toLongValue(resultMap.get("connectionId")))
                .sinkConnectorId(toLongValue(resultMap.get("sinkConnectorId")))
                .sourceConnectorId(toLongValue(resultMap.get("sourceConnectorId")))
                .sourceDataSystemType(DataSystemType.codeOf(toIntValue(resultMap.get("sourceDataSystemType"))))
                .sinkDataSystemType(DataSystemType.codeOf(toIntValue(resultMap.get("sinkDataSystemType"))))
                .desiredState(ConnectionState.codeOf(toIntValue(resultMap.get("desiredState"))))
                .actualState(ConnectionState.codeOf(toIntValue(resultMap.get("actualState"))))
                .requisitionState(RequisitionState.codeOf(toIntValue(resultMap.get("requisitionState"))))
                .creationTimeFormat(DateUtil.formatToString(toInstant(resultMap.get("creationTime"))))
                .updateTimeFormat(DateUtil.formatToString(toInstant(resultMap.get("updateTime"))))
                .sourceProjectId(toLongValue(resultMap.get("sourceProjectId")))
                .sinkProjectId(toLongValue(resultMap.get("sinkProjectId")))
                .sourceDatasetClusterId(toLongValue(resultMap.get("sourceDatasetClusterId")))
                .sinkDatasetClusterId(toLongValue(resultMap.get("sinkDatasetClusterId")))
                .sourceDatasetDatabaseId(toLongValue(resultMap.get("sourceDatasetDatabaseId")))
                .sinkDatasetDatabaseId(toLongValue(resultMap.get("sinkDatasetDatabaseId")))
                .sourceDatasetId(toLongValue(resultMap.get("sourceDatasetId")))
                .sinkDatasetId(toLongValue(resultMap.get("sinkDatasetId")))
                .sinkDatasetInstanceId(toLongValue(resultMap.get("sinkDatasetInstanceId")))
                .sourceProjectName(toStringValue(resultMap.get("sourceProjectName")))
                .sinkProjectName(toStringValue(resultMap.get("sinkProjectName")))
                .sourceDatasetClusterName(toStringValue(resultMap.get("sourceDatasetClusterName")))
                .sinkDatasetClusterName(toStringValue(resultMap.get("sinkDatasetClusterName")))
                .sourceDatasetDatabaseName(toStringValue(resultMap.get("sourceDatasetDatabaseName")))
                .sinkDatasetDatabaseName(toStringValue(resultMap.get("sinkDatasetDatabaseName")))
                .sourceDatasetName(toStringValue(resultMap.get("sourceDatasetName")))
                .sinkDatasetName(toStringValue(resultMap.get("sinkDatasetName")))
                .sinkDatasetInstanceName(toStringValue(resultMap.get("sinkDatasetInstanceName")))
                .build();
    }

    private static Long toLongValue(final Object result) {
        if (Objects.isNull(result)) {
            return Long.MIN_VALUE;
        }
        BigInteger bigInt = (BigInteger) result;
        return bigInt.longValue();
    }

    private static Instant toInstant(final Object result) {
        Timestamp timestamp = (Timestamp) result;
        return timestamp.toInstant();
    }

    private static String toStringValue(final Object result) {
        return String.valueOf(result);
    }

    private static Integer toIntValue(final Object result) {
        return Integer.valueOf(String.valueOf(result));
    }
}
