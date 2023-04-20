package cn.xdf.acdc.devops.service.process.tool.command;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.ConnectClusterRepository;
import cn.xdf.acdc.devops.repository.ConnectorClassRepository;
import cn.xdf.acdc.devops.repository.DefaultConnectorConfigurationRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.tool.command.ApiConnectCommand.CommandEntity.ClusterType;
import cn.xdf.acdc.devops.service.process.tool.command.ApiConnectCommand.CommandEntity.Operation;
import cn.xdf.acdc.devops.service.util.UIError;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Connect;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class ApiConnectCommand implements Command<ApiConnectCommand.CommandEntity> {

    private static final String DEFAULT_CONNECT_CLUSTER_VERSION = "v1";

    private static final String VALUE_SCHEMA_REGISTRY_URL_KEY = "value.converter.schema.registry.url";

    private static final String KEY_SCHEMA_REGISTRY_URL_KEY = "key.converter.schema.registry.url";

    //source-mysql
    private static final String DATABASE_HISTORY_PRODUCER_SECURITY_PROTOCOL = "database.history.producer.security.protocol";

    private static final String DATABASE_HISTORY_CONSUMER_SECURITY_PROTOCOL = "database.history.consumer.security.protocol";

    private static final String DATABASE_HISTORY_PRODUCER_SASL_MECHANISM = "database.history.producer.sasl.mechanism";

    private static final String DATABASE_HISTORY_CONSUMER_SASL_MECHANISM = "database.history.consumer.sasl.mechanism";

    // source-tidb
    private static final String SOURCE_KAFKA_SECURITY_PROTOCOL = "source.kafka.security.protocol";

    private static final String SOURCE_KAFKA_SASL_MECHANISM = "source.kafka.sasl.mechanism";

    private static final String PLAINTEXT = "PLAINTEXT";

    private static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";

    private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";

    private static final String SCRAM_SHA_256 = "SCRAM-SHA-256";

    private static final String PLAIN = "PLAIN";

    private static final String SOURCE_MYSQL_CLASS = "io.debezium.connector.mysql.MySqlConnector";

    private static final String SOURCE_TIDB_CLASS = "cn.xdf.acdc.connector.tidb.TidbConnector";

    private static final Set<String> KAFKA_SECURITY_PROTOCOL_SET = Sets.newHashSet(

            PLAINTEXT,
            SASL_PLAINTEXT
    );

    private static final Set<String> KAFKA_SASL_MECHANISM_SET = Sets.newHashSet(
            SCRAM_SHA_512,
            SCRAM_SHA_256,
            PLAIN
    );

    private static final Set<String> IGNORE_CONFIG_KEY_SET = Sets.newHashSet(
            "connector.class"
    );

    // CHECKSTYLE:OFF
    private static final Map<String, String> SOURCE_MYSQL_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = -8838983215782575828L;

        {
            put("database.history.producer.sasl.mechanism", "");
            put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
            put("transforms.unwrap.delete.handling.mode", "rewrite");
            put("database.history.producer.security.protocol", "PLAINTEXT");
            put("tasks.max", "1");
            put("transforms", "route,unwrap");
            put("time.precision.mode", "connect");
            put("database.history.consumer.security.protocol", "PLAINTEXT");
            put("transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter");
            put("transforms.route.regex", "([^.]+)\\.([^.]+)\\.([^.]+)");
            put("transforms.unwrap.add.fields", "op,table");
            put("producer.compression.type", "lz4");
            put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("transforms.route.replacement", "$1-$3");
            put("database.history.consumer.sasl.mechanism", "");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
            put("snapshot.mode", "schema_only");
        }
    };

    private static final Map<String, String> SOURCE_TIDB_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = -6049957650004881611L;

        {
            put("connector.class", "cn.xdf.acdc.connector.tidb.TidbConnector");
            put("transforms.unwrap.delete.handling.mode", "rewrite");
            put("tasks.max", "3");
            put("time.precision.mode", "connect");
            put("transforms", "route,unwrap");
            put("source.kafka.sasl.mechanism", "");
            put("transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter");
            put("transforms.route.regex", "([^.]+)\\.([^.]+)\\.([^.]+)");
            put("source.kafka.reader.thread.number", "4");
            put("source.kafka.session.timeout.ms", "30000");
            put("producer.compression.type", "lz4");
            put("transforms.unwrap.add.fields", "op");
            put("source.kafka.security.protocol", "PLAINTEXT");
            put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("transforms.route.replacement", "$1-$3");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
        }
    };

    private static final Map<String, String> SINK_MYSQL_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = 5769648312915755359L;

        {
            put("connector.class", "cn.xdf.acdc.connect.jdbc.JdbcSinkConnector");
            put("tasks.max", "1");
            put("db.timezone", "Asia/Shanghai");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
        }
    };

    private static final Map<String, String> SINK_TIDB_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = 4545453948867312049L;

        {
            put("connector.class", "cn.xdf.acdc.connect.jdbc.JdbcSinkConnector");
            put("tasks.max", "1");
            put("db.timezone", "Asia/Shanghai");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
        }
    };

    private static final Map<String, String> SINK_HIVE_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = 2548897393104220959L;

        {
            put("connector.class", "cn.xdf.acdc.connect.hdfs.HdfsSinkConnector");
            put("flush.size", "1");
            put("timezone", "Asia/Shanghai");
            put("storage.root.path", "cdc/connectors");
            put("tasks.max", "1");
            put("transforms", "InsertSource,ValueMapperSource");
            put("hive.schema.change.support", "false");
            put("locale", "zh-CN");
            put("hive.integration.mode", "WITH_HIVE_META_DATA");
            put("db.timezone", "Asia/Shanghai");
            put("transforms.InsertSource.offset.field", "__kafka_record_offset");
            put("transforms.ValueMapperSource.mappings", "c:I,u:U,d:D");
            put("storage.format", "TEXT");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
            put("partition.duration.ms", "1000");
            put("batch.size", "1000");
            put("rotation.policy", "FILE_SIZE");
            put("transforms.ValueMapperSource.field", "__op");
            put("partitioner.class", "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
            put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
            put("path.format", "'etldate'=yyyyMMdd");
            put("rotate.schedule.interval.ms", "60000");
            put("storage.mode", "AT_LEAST_ONCE");
            put("transforms.ValueMapperSource.type", "cn.xdf.acdc.connect.smt.valuemapper.StringValueMapper");
        }
    };

    private static final Map<String, String> SINK_KAFKA_DEFAULT_CONF = new HashMap<String, String>() {
        private static final long serialVersionUID = 5793182411505027416L;

        {
            put("connector.class", "cn.xdf.acdc.connect.kafka.KafkaSinkConnector");
            put("tasks.max", "1");
            put("db.timezone", "Asia/Shanghai");
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("key.converter", "io.confluent.connect.avro.AvroConverter");
        }
    };
    // CHECKSTYLE:ON

    private final Map<CommandEntity.Operation, Function<CommandEntity, Map<String, Object>>> commandExecutors = new HashMap<>();

    @Autowired
    private I18nService i18n;

    @Autowired
    private ConnectClusterRepository connectClusterRepository;

    @Autowired
    private ConnectorClassRepository connectorClassRepository;

    @Autowired
    private DefaultConnectorConfigurationRepository defaultConnectorConfigurationRepository;

    public ApiConnectCommand() {
        commandExecutors.put(Operation.CREATE, this::doCreate);
        commandExecutors.put(Operation.DELETE, this::doDelete);
        commandExecutors.put(Operation.UPDATE_DEFAULT_CONFIG, this::doUpdateDefaultConfig);
        commandExecutors.put(Operation.GET, this::doGet);
        commandExecutors.put(Operation.LIST, this::doList);
    }

    @Override
    public Map<String, Object> execute(final CommandEntity entity) {
        return commandExecutors.getOrDefault(entity.opt, this::doNothing).apply(entity);
    }

    private Map<String, Object> doGet(final CommandEntity entity) {

        if (Strings.isNullOrEmpty(entity.clusterServer)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
        }

        ConnectClusterDO cluster = connectClusterRepository.findOneByConnectRestApiUrl(entity.clusterServer)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connect.CLUSTER_NOT_FOUND, entity.clusterServer)));

        List<DefaultConnectorConfigurationDO> defaultConnectorConfList = cluster.getConnectorClass().getDefaultConnectorConfigurations()
                .stream().collect(Collectors.toList());

        ConnectorClassDO connectorClass = cluster.getConnectorClass();
        Map<String, Object> clusterBody = new LinkedHashMap<>();
        clusterBody.put("id", cluster.getId());
        clusterBody.put("connectorType", connectorClass.getConnectorType());
        clusterBody.put("dataSystemType", connectorClass.getDataSystemType());
        clusterBody.put("server", cluster.getConnectRestApiUrl());
        clusterBody.put("defaultConfig", convertToConnectorConfigurationMap(defaultConnectorConfList));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cluster", clusterBody);
        return result;
    }

    private Map<String, Object> doList(final CommandEntity entity) {

        List<ConnectClusterDO> connectClusters = connectClusterRepository.findAll();

        List<Map<String, Object>> clusters = connectClusters.stream().map(it -> {
            ConnectorClassDO connectorClass = it.getConnectorClass();

            List<DefaultConnectorConfigurationDO> defaultConnectorConfList = it.getConnectorClass().getDefaultConnectorConfigurations()
                    .stream().collect(Collectors.toList());

            Map<String, Object> cluster = new LinkedHashMap<>();
            cluster.put("id", it.getId());
            cluster.put("server", it.getConnectRestApiUrl());
            cluster.put("connectorType", connectorClass.getConnectorType());
            cluster.put("dataSystemType", connectorClass.getDataSystemType());
            cluster.put("defaultConfig", convertToConnectorConfigurationMap(defaultConnectorConfList));

            return cluster;
        }).collect(Collectors.toList());

        Map<String, Object> mockCluster = new HashMap<>();
        mockCluster.put("id", "1");
        mockCluster.put("server", "0");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("clusters", clusters);
        return result;
    }

    private Map<String, Object> doCreate(final CommandEntity entity) {

        if (Objects.isNull(entity.clusterType)
                || Strings.isNullOrEmpty(entity.clusterServer)
                || Strings.isNullOrEmpty(entity.schemaRegistryUrl)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
        }

        // There can only be one: "SOURCE_MYSQL", "SOURCE_TIDB", "SINK_MYSQL", "SINK_TIDB", "SINK_HIVE", "SINK_KAFKA".
        ClusterType clusterType = entity.clusterType;
        connectorClassRepository.findOneByNameAndDataSystemType(clusterType.connectorClass, clusterType.getDataSystemType())
                .ifPresent(connectorClass -> {
                    connectClusterRepository.findOneByConnectorClassId(connectorClass.getId()).ifPresent(cluster -> {
                        throw new EntityExistsException(i18n.msg(Connect.CLUSTER_ALREADY_EXISTED, cluster.getConnectRestApiUrl()));
                    });
                });

        // Cluster addresses must be unique.
        connectClusterRepository.findOneByConnectRestApiUrl(entity.clusterServer).ifPresent(cluster -> {
            throw new EntityExistsException(i18n.msg(Connect.CLUSTER_ALREADY_EXISTED, cluster.getConnectRestApiUrl()));
        });

        // class
        ConnectorClassDO connectorClass = ConnectorClassDO.builder()
                .connectorType(clusterType.connectorType)
                .dataSystemType(clusterType.dataSystemType)
                .description(clusterType.name())
                .simpleName(clusterType.name())
                .name(clusterType.connectorClass)
                .build();

        ConnectorClassDO savedConnectorClass = connectorClassRepository.save(connectorClass);

        // config
        Set<DefaultConnectorConfigurationDO> defaultConnectorConfSet = convertToConnectorConfigurationSet(entity, savedConnectorClass.getId());
        List<DefaultConnectorConfigurationDO> savedDefaultConnectorConfList = defaultConnectorConfigurationRepository.saveAll(defaultConnectorConfSet);

        ConnectClusterDO connectCluster = ConnectClusterDO.builder()
                .connectRestApiUrl(entity.clusterServer.trim())
                .description(clusterType.name())
                .version(DEFAULT_CONNECT_CLUSTER_VERSION)
                .connectorClass(savedConnectorClass)
                .build();

        // cluster
        ConnectClusterDO savedCluster = connectClusterRepository.save(connectCluster);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", savedCluster.getId());
        result.put("server", savedCluster.getConnectRestApiUrl());
        result.put("defaultConfig", convertToConnectorConfigurationMap(savedDefaultConnectorConfList));
        return result;
    }

    private Map<String, Object> doUpdateDefaultConfig(final CommandEntity entity) {
        // check
        if (Strings.isNullOrEmpty(entity.clusterServer)
                || Strings.isNullOrEmpty(entity.key)
                || Strings.isNullOrEmpty(entity.value)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
        }

        if (IGNORE_CONFIG_KEY_SET.contains(entity.key)) {
            throw new ClientErrorException(i18n.msg(I18nKey.Connect.CLUSTER_DEFAULT_CONFIG_ERROR_MODIFICATION, entity.key));
        }

        ConnectClusterDO connectCluster = connectClusterRepository.findOneByConnectRestApiUrl(entity.clusterServer)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connect.CLUSTER_NOT_FOUND, entity.clusterServer)));

        ConnectorClassDO connectorClass = connectCluster.getConnectorClass();

        DefaultConnectorConfigurationDO defaultConf = defaultConnectorConfigurationRepository.findByConnectorClassIdAndName(connectorClass.getId(), entity.key)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connect.CLUSTER_DEFAULT_CONFIG_NOT_FOUND, entity.key)));

        String oldValue = defaultConf.getValue();
        defaultConf.setValue(entity.value.trim());
        DefaultConnectorConfigurationDO savedDefaultConf = defaultConnectorConfigurationRepository.save(defaultConf);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", entity.key);
        result.put("oldValue", oldValue);
        result.put("newValue", savedDefaultConf.getValue());
        return result;
    }

    private Map<String, Object> doDelete(final CommandEntity entity) {
        // check

        if (Strings.isNullOrEmpty(entity.clusterServer)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
        }

        ConnectClusterDO connectCluster = connectClusterRepository.findOneByConnectRestApiUrl(entity.clusterServer)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connect.CLUSTER_NOT_FOUND, entity.clusterServer)));

        Set<ConnectorDO> connectors = connectCluster.getConnectors();

        if (!CollectionUtils.isEmpty(connectors)) {
            throw new ClientErrorException(i18n.msg(Connect.CLUSTER_DELETE_CLUSTER_WITH_CONNECTORS));
        }

        ConnectorClassDO connectorClass = connectCluster.getConnectorClass();
        Set<DefaultConnectorConfigurationDO> defaultConfSet = connectorClass.getDefaultConnectorConfigurations();
        defaultConnectorConfigurationRepository.deleteInBatch(defaultConfSet);
        connectClusterRepository.deleteById(connectCluster.getId());
        connectorClassRepository.deleteById(connectorClass.getId());

        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doNothing(final CommandEntity entity) {
        return UIError.getBriefStyleMsg(HttpStatus.BAD_REQUEST, i18n.msg(I18nKey.Command.OPERATION_NOT_SPECIFIED, String.valueOf(entity.opt)));
    }

    private void setSourceMysqlKafkaSASL(final CommandEntity entity, final Map<String, String> config) {
        checkSaslConfig(entity);
        String securityProtocol = entity.securityProtocol;
        String saslMechanism = entity.saslMechanism;
        config.put(DATABASE_HISTORY_PRODUCER_SECURITY_PROTOCOL, securityProtocol);
        config.put(DATABASE_HISTORY_CONSUMER_SECURITY_PROTOCOL, securityProtocol);

        if (SASL_PLAINTEXT.equals(securityProtocol)) {
            config.put(DATABASE_HISTORY_PRODUCER_SASL_MECHANISM, saslMechanism);
            config.put(DATABASE_HISTORY_CONSUMER_SASL_MECHANISM, saslMechanism);
        }
    }

    private void setSourceTidbKafkaSASL(final CommandEntity entity, final Map<String, String> config) {
        checkSaslConfig(entity);
        String securityProtocol = entity.securityProtocol;
        String saslMechanism = entity.saslMechanism;

        config.put(SOURCE_KAFKA_SECURITY_PROTOCOL, securityProtocol);
        if (SASL_PLAINTEXT.equals(securityProtocol)) {
            config.put(SOURCE_KAFKA_SASL_MECHANISM, saslMechanism);
        }
    }

    private void checkSaslConfig(final CommandEntity entity) {
        String securityProtocol = entity.securityProtocol;
        String saslMechanism = entity.saslMechanism;

        if (!KAFKA_SECURITY_PROTOCOL_SET.contains(securityProtocol)) {
            if (Strings.isNullOrEmpty(entity.clusterServer)) {
                throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
            }
        }
        if (SASL_PLAINTEXT.equals(securityProtocol) && !KAFKA_SASL_MECHANISM_SET.contains(saslMechanism)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, entity.toString()));
        }
    }

    private Set<DefaultConnectorConfigurationDO> convertToConnectorConfigurationSet(
            final CommandEntity entity,
            final Long connectorClassId) {
        ClusterType clusterType = entity.clusterType;

        String schemaRegistryUrl = entity.schemaRegistryUrl.trim();
        Map<String, String> newConfigMap = new HashMap<>(clusterType.getDefaultConf());
        newConfigMap.put(VALUE_SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        newConfigMap.put(KEY_SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);

        if (isMysqlSource(clusterType.connectorClass)) {
            setSourceMysqlKafkaSASL(entity, newConfigMap);
        }

        if (isTidbSource(clusterType.connectorClass)) {
            setSourceTidbKafkaSASL(entity, newConfigMap);
        }

        return newConfigMap.entrySet().stream()
                .map(it -> DefaultConnectorConfigurationDO.builder()
                        .name(it.getKey())
                        .value(it.getValue())
                        .connectorClass(ConnectorClassDO.builder().id(connectorClassId).build())
                        .build()
                ).collect(Collectors.toSet());
    }

    private Map<String, String> convertToConnectorConfigurationMap(final List<DefaultConnectorConfigurationDO> configList) {
        return configList.stream()
                .collect(Collectors.toMap(conf -> conf.getName(), conf -> conf.getValue()));
    }

    private boolean isMysqlSource(final String connectorClass) {
        return SOURCE_MYSQL_CLASS.equals(connectorClass);
    }

    private boolean isTidbSource(final String connectorClass) {
        return SOURCE_TIDB_CLASS.equals(connectorClass);
    }

    // CHECKSTYLE:OFF
    public static class CommandEntity {

        // --create, --delete , --update-default-config
        private Operation opt;

        // The cluster type
        private ClusterType clusterType;

        // The default configuration item is key
        private String key;

        // The default configuration item is value
        private String value;

        // The connect cluster server, eg: localhost:8083
        private String clusterServer;

        // The schema registry url eg: localhost:8081
        private String schemaRegistryUrl;

        private String securityProtocol;

        private String saslMechanism;

        public Operation getOpt() {
            return opt;
        }

        public void setOpt(Operation opt) {
            this.opt = opt;
        }

        public ClusterType getClusterType() {
            return clusterType;
        }

        public void setClusterType(ClusterType clusterType) {
            this.clusterType = clusterType;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getClusterServer() {
            return clusterServer;
        }

        public void setClusterServer(String clusterServer) {
            this.clusterServer = clusterServer;
        }

        public String getSchemaRegistryUrl() {
            return schemaRegistryUrl;
        }

        public void setSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("opt:").append(opt).append(" ");
            sb.append("clusterType:").append(clusterType).append(" ");
            sb.append("clusterServer:").append(clusterServer).append(" ");
            sb.append("schemaRegistryUrl:").append(schemaRegistryUrl).append(" ");
            sb.append("securityProtocol:").append(securityProtocol).append(" ");
            sb.append("saslMechanism:").append(saslMechanism).append(" ");
            sb.append("key:").append(key).append(" ");
            sb.append("value:").append(value).append(" ");
            return sb.toString();
        }

        public enum Operation {
            CREATE, DELETE, UPDATE_DEFAULT_CONFIG, GET, LIST
        }

        @Getter
        public enum ClusterType {
            SOURCE_MYSQL("io.debezium.connector.mysql.MySqlConnector", SOURCE_MYSQL_DEFAULT_CONF, DataSystemType.MYSQL, ConnectorType.SOURCE),

            SOURCE_TIDB("cn.xdf.acdc.connector.tidb.TidbConnector", SOURCE_TIDB_DEFAULT_CONF, DataSystemType.TIDB, ConnectorType.SOURCE),

            SINK_MYSQL("cn.xdf.acdc.connect.jdbc.JdbcSinkConnector", SINK_MYSQL_DEFAULT_CONF, DataSystemType.MYSQL, ConnectorType.SINK),

            SINK_TIDB("cn.xdf.acdc.connect.jdbc.JdbcSinkConnector", SINK_TIDB_DEFAULT_CONF, DataSystemType.TIDB, ConnectorType.SINK),

            SINK_HIVE("cn.xdf.acdc.connect.hdfs.HdfsSinkConnector", SINK_HIVE_DEFAULT_CONF, DataSystemType.HIVE, ConnectorType.SINK),

            SINK_KAFKA("cn.xdf.acdc.connect.kafka.KafkaSinkConnector", SINK_KAFKA_DEFAULT_CONF, DataSystemType.KAFKA, ConnectorType.SINK);

            private Map<String, String> defaultConf;

            private String connectorClass;

            private DataSystemType dataSystemType;

            private ConnectorType connectorType;


            ClusterType(
                    final String connectorClass,
                    final Map<String, String> defaultConf,
                    final DataSystemType dataSystemType,
                    final ConnectorType connectorType
            ) {
                this.connectorClass = connectorClass;
                this.defaultConf = defaultConf;
                this.dataSystemType = dataSystemType;
                this.connectorType = connectorType;
            }
        }
    }
}
