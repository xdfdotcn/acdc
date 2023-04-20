package cn.xdf.acdc.devops.service.process.tool.command;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaConstant;
import cn.xdf.acdc.devops.service.process.tool.command.ApiKafkaClusterCommand.CommandEntity.Operation;
import cn.xdf.acdc.devops.service.util.UIError;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

@Component
public class ApiKafkaClusterCommand implements Command<ApiKafkaClusterCommand.CommandEntity> {

    private final Map<CommandEntity.Operation, Function<CommandEntity, Map<String, Object>>> commandExecutors = new HashMap<>();

    @Autowired
    private I18nService i18n;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaHelperService kafkaHelperService;

    public ApiKafkaClusterCommand() {
        commandExecutors.put(Operation.CREATE, this::doCreate);
        commandExecutors.put(Operation.DELETE, this::doDelete);
        commandExecutors.put(Operation.GET, this::doGet);
    }

    @Override
    public Map<String, Object> execute(final CommandEntity entity) {
        return commandExecutors.getOrDefault(entity.opt, this::doNothing).apply(entity);
    }

    private Map<String, Object> doCreate(final CommandEntity entity) {
        Map<String, Object> securityConfig = buildSecurityConfig(entity);

        KafkaClusterDTO kafkaCluster = kafkaClusterService.create(
                KafkaClusterDTO.builder()
                        .bootstrapServers(entity.bootstrapServer)
                        .clusterType(entity.clusterType)
                        .description(entity.clusterType.name())
                        .name(entity.clusterType.name())
                        .securityProtocol(entity.securityProtocol)
                        .saslMechanism(entity.saslMechanism)
                        .saslUsername(entity.saslUsername)
                        .saslPassword(entity.saslPassword)
                        .version(entity.kafkaVersion)
                        .build(),
                securityConfig
        );

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", kafkaCluster.getId());
        result.put("name", kafkaCluster.getName());
        result.put("bootstrapServers", kafkaCluster.getBootstrapServers());
        return result;
    }

    private Map<String, Object> doDelete(final CommandEntity entity) {
        Optional<KafkaClusterDTO> optional = kafkaClusterService.getByBootstrapServers(entity.bootstrapServer);
        if (optional.isPresent()) {
            kafkaClusterService.deleteById(optional.get().getId());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doGet(final CommandEntity entity) {
        Optional<KafkaClusterDTO> optional = kafkaClusterService.getByBootstrapServers(entity.bootstrapServer);
        Map<String, Object> result = new LinkedHashMap<>();
        if (optional.isPresent()) {
            result.put("id", optional.get().getId());
            result.put("name", optional.get().getName());
            result.put("bootstrapServers", optional.get().getBootstrapServers());
        }
        return result;
    }

    private Map<String, Object> doNothing(final CommandEntity entity) {
        return UIError.getBriefStyleMsg(HttpStatus.BAD_REQUEST, i18n.msg(I18nKey.Command.OPERATION_NOT_SPECIFIED, String.valueOf(entity.opt)));
    }

    private Map<String, Object> buildSecurityConfig(final CommandEntity entity) {
        Map<String, Object> config = Maps.newHashMapWithExpectedSize(3);

        String securityProtocol = entity.getSecurityProtocol();
        if (StringUtils.equals(KafkaConstant.SECURITY_PROTOCOL_SASL_PLAINTEXT, securityProtocol)) {
            String username = entity.getSaslUsername();
            String password = entity.getSaslPassword();

            String saslJaasConfig = Objects.equals(KafkaConstant.SASL_MECHANISM_PLAIN, entity.getSaslMechanism())
                    ? String.format(KafkaConstant.SASL_JAAS_CONFIG_PATTERN, KafkaConstant.PLAIN_LOGIN_MODULE_CLASS, username, password)
                    : String.format(KafkaConstant.SASL_JAAS_CONFIG_PATTERN, KafkaConstant.SCRAM_LOGIN_MODULE_CLASS, username, password);

            config.put(SaslConfigs.SASL_MECHANISM, entity.getSaslMechanism());
            config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }

        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        return config;
    }

    // CHECKSTYLE:OFF
    public static class CommandEntity {

        private Operation opt;

        private String bootstrapServer;

        private KafkaClusterType clusterType;

        private String securityProtocol;

        private String saslMechanism;

        private String saslUsername;

        private String saslPassword;

        private String kafkaVersion;

        public Operation getOpt() {
            return opt;
        }

        public void setOpt(Operation opt) {
            this.opt = opt;
        }

        public String getBootstrapServer() {
            return bootstrapServer;
        }

        public void setBootstrapServer(String bootstrapServer) {
            this.bootstrapServer = bootstrapServer;
        }

        public KafkaClusterType getClusterType() {
            return clusterType;
        }

        public void setClusterType(KafkaClusterType clusterType) {
            this.clusterType = clusterType;
        }

        public String getSecurityProtocol() {
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public void setSaslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
        }

        public String getSaslUsername() {
            return saslUsername;
        }

        public void setSaslUsername(String saslUsername) {
            this.saslUsername = saslUsername;
        }

        public String getSaslPassword() {
            return saslPassword;
        }

        public void setSaslPassword(String saslPassword) {
            this.saslPassword = saslPassword;
        }

        public String getKafkaVersion() {
            return kafkaVersion;
        }

        public void setKafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("opt:").append(opt).append(" ");
            sb.append("bootstrapServer:").append(bootstrapServer).append(" ");
            sb.append("clusterType:").append(clusterType).append(" ");
            sb.append("securityProtocol:").append(securityProtocol).append(" ");
            sb.append("saslMechanism:").append(saslMechanism).append(" ");
            sb.append("saslUsername:").append(saslUsername).append(" ");
            sb.append("saslPassword:").append(saslPassword).append(" ");
            return sb.toString();
        }

        public enum Operation {
            CREATE, DELETE, GET
        }
    }

}
