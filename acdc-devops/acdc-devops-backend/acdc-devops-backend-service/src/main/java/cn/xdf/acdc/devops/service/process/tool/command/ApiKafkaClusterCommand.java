package cn.xdf.acdc.devops.service.process.tool.command;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import cn.xdf.acdc.devops.service.process.tool.command.ApiKafkaClusterCommand.CommandEntity.Operation;
import cn.xdf.acdc.devops.service.util.UIError;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

@Component
public class ApiKafkaClusterCommand implements Command<ApiKafkaClusterCommand.CommandEntity> {

    @Autowired
    private I18nService i18n;

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    private final Map<CommandEntity.Operation, Function<CommandEntity, Map<String, Object>>> commandExecutors = new HashMap<>();

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
        KafkaClusterDTO kafkaCluster = kafkaClusterProcessService.saveInternalKafkaCluster(
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
                        .build()
        );

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", kafkaCluster.getId());
        result.put("name", kafkaCluster.getName());
        result.put("bootstrapServers", kafkaCluster.getBootstrapServers());
        return result;
    }

    private Map<String, Object> doDelete(final CommandEntity entity) {
        kafkaClusterProcessService.deleteInternalKafkaCluster(entity.bootstrapServer);
        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doGet(final CommandEntity entity) {
        KafkaClusterDTO kafkaCluster = kafkaClusterProcessService.getKafkaClusterByBootstrapServers(entity.bootstrapServer);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", kafkaCluster.getId());
        result.put("name", kafkaCluster.getName());
        result.put("bootstrapServers", kafkaCluster.getBootstrapServers());
        return result;
    }

    private Map<String, Object> doNothing(final CommandEntity entity) {
        return UIError.getBriefStyleMsg(HttpStatus.BAD_REQUEST, i18n.msg(I18nKey.Command.OPERATION_NOT_SPECIFIED, String.valueOf(entity.opt)));
    }

    // CHECKSTYLE:OFF
    public static class CommandEntity {

        public enum Operation {
            CREATE, DELETE, GET
        }

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
    }

}
