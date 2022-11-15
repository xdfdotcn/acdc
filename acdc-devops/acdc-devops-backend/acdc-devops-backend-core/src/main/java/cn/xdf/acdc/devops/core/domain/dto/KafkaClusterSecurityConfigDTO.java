package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka 安全认证.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaClusterSecurityConfigDTO {

    private String securityProtocolConfig;

    private String saslMechanism;

    private String saslJaasConfig;
}
