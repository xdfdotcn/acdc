/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.common.ComposableConfig;
import cn.xdf.acdc.connect.hdfs.common.GenericRecommender;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.DailyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.FieldPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.HourlyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

public class HdfsSinkConfig extends StorageSinkConnectorConfig {

    // HDFS Group
    // This config is deprecated and will be removed in future releases. Use store.url instead.
    public static final String HDFS_URL_CONFIG = "hdfs.url";

    public static final String HDFS_URL_DOC =
        "The HDFS connection URL. This configuration has the format of hdfs://hostname:port and "
            + "specifies the HDFS to export data to. This property is deprecated and will be "
            + "removed in future releases. Use ``store.url`` instead.";

    public static final String HDFS_URL_DEFAULT = null;

    public static final String HDFS_URL_DISPLAY = "HDFS URL";

    public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";

    public static final String HADOOP_CONF_DIR_DEFAULT = "";

    public static final String HADOOP_CONF_DIR_DOC = "The Hadoop configuration directory.";

    public static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

    public static final String HADOOP_HOME_CONFIG = "hadoop.home";

    public static final String HADOOP_HOME_DEFAULT = "";

    public static final String HADOOP_HOME_DOC = "The Hadoop home directory.";

    public static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";

    // HDFS Group
    // storage root directory
    public static final String STORAGE_ROOT_PATH = "storage.root.path";

    public static final String STORAGE_ROOT_PATH_DOC = "The root directory to store the write record.";

    public static final String STORAGE_ROOT_PATH_DEFAULT = "/tmp/connectors";

    public static final String STORAGE_ROOT_PATH_DISPLAY = "Storage root directory";

    // hive integration mode
    public static final String HIVE_INTEGRATION_MODE = "hive.integration.mode";

    public static final String HIVE_INTEGRATION_MODE_DOC = "Hive integration mode, There are three kinds of mode:"
        + "WITH_HIVE_META_DATA,AUTO_CREATE_EXTERNAL_TABLE,NONE.";

    public static final String HIVE_INTEGRATION_MODE_DEFAULT = "NONE";

    public static final String HIVE_INTEGRATION_MODE_DISPLAY = "Hive integration mode";

    // schema change
    public static final String HIVE_SCHEMA_CHANGE_SUPPORT = "hive.schema.change.support";

    public static final String HIVE_SCHEMA_CHANGE_SUPPORT_DOC = "When hive integration mode is WITH_HIVE_META_DATA,this options take effect,"
        + " whether to support alert table when schema change.";

    public static final boolean HIVE_SCHEMA_CHANGE_SUPPORT_DEFAULT = false;

    public static final String HIVE_SCHEMA_CHANGE_SUPPORT_DISPLAY = "Hive schema change support";

    // storage mode
    public static final String STORAGE_MODE = "storage.mode";

    public static final String STORAGE_MODE_DOC = "Storage mode,there are two kinds of mode:EXACTLY_ONCE,AT_LEAST_ONCE.";

    public static final String STORAGE_MODE_DEFAULT = "EXACTLY_ONCE";

    public static final String STORAGE_MODE_DISPLAY = "Storage mode";

    // storage format
    public static final String STORAGE_FORMAT = "storage.format";

    public static final String STORAGE_FORMAT_DOC = "Support format type: AVRO,ORC,JSON,STRING,TXT,PARQUET";

    public static final String STORAGE_FORMAT_DEFAULT = "AVRO";

    public static final String STORAGE_FORMAT_DISPLAY = "Storage format";

    // storage format text separator
    public static final String STORAGE_FORMAT_TEXT_SEPARATOR = "storage.format.text.separator";

    public static final String STORAGE_FORMAT_TEXT_SEPARATOR_DOC = "Store as text,need specified separator,eg:','";

    public static final String STORAGE_FORMAT_TEXT_SEPARATOR_DEFAULT = "\001";

    public static final String STORAGE_FORMAT_TEXT_SEPARATOR_DISPLAY = "Text format separator";

    //rotation policy
    public static final String ROTATION_POLICY = "rotation.policy";

    public static final String ROTATION_POLICY_DOC = "File rotation policy : BLOCK_SIZE,RECORD_SIZE";

    public static final String ROTATION_POLICY_DEFAULT = "FILE_SIZE";

    public static final String ROTATION_POLICY_DISPLAY = "File rotation policy";

    //  rotation policy file size
    public static final String ROTATION_POLICY_FILE_SIZE = "rotation.policy.file.size";

    public static final String ROTATION_POLICY_FILE_SIZE_DOC = "File rotation by file size,default 500M";

    public static final String ROTATION_POLICY_FILE_SIZE_DEFAULT = "524288000";

    public static final String ROTATION_POLICY_BLOCK_SIZE_DISPLAY = "Rotation file size";

    // rotation policy record size
    // TODO 每次批量处理完成，都会关闭流，此策略不可用，待后续重新设计
    public static final String ROTATION_POLICY_RECORD_SIZE = "rotation.policy.record.size";

    public static final String ROTATION_POLICY_RECORD_SIZE_DOC = "File rotation by record size,default 100000 ";

    public static final String ROTATION_POLICY_RECORD_SIZE_DEFAULT = "100000";

    public static final String ROTATION_POLICY_RECORD_SIZE_DISPLAY = "Rotation record size";

    // hadoop user
    public static final String HADOOP_USER = "hadoop.user";

    public static final String HADOOP_USER_DOC = "Hadoop user";

    public static final String HADOOP_USER_DEFAULT = "hive";

    public static final String HADOOP_USER_DISPLAY = "Hadoop user name";


    // Security group
    public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";

    public static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
        "Configuration indicating whether HDFS is using Kerberos for authentication.";

    public static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;

    public static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

    public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";

    public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";

    public static final String CONNECT_HDFS_PRINCIPAL_DOC =
        "The principal to use when HDFS is using Kerberos to for authentication.";

    public static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

    public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";

    public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";

    public static final String CONNECT_HDFS_KEYTAB_DOC =
        "The path to the keytab file for the HDFS connector principal. "
            + "This keytab file should only be readable by the connector user.";

    public static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

    public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";

    public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";

    public static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";

    public static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

    public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG =
        "kerberos.ticket.renew.period.ms";

    public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;

    public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
        "The period in milliseconds to renew the Kerberos ticket.";

    public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew "
        + "Period (ms)";

    public static final ConfigDef.Recommender HDFS_AUTHENTICATION_KERBEROS_DEPENDENTS_RECOMMENDER =
        new BooleanParentRecommender(
            HDFS_AUTHENTICATION_KERBEROS_CONFIG);

    private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();

    private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();

    private static final String HDFS_CONF_PREFIX = "__hdfs.";

    static {
        STORAGE_CLASS_RECOMMENDER.addValidValues(
            Arrays.asList(HdfsStorage.class)
        );

        PARTITIONER_CLASS_RECOMMENDER.addValidValues(
            Arrays.asList(
                DefaultPartitioner.class,
                HourlyPartitioner.class,
                DailyPartitioner.class,
                TimeBasedPartitioner.class,
                FieldPartitioner.class
            )
        );
        // Define HDFS configuration group
        final String hdfsGroup = "HDFS";
        int orderInHdfsGroup = 0;

        // HDFS_URL_CONFIG property is retained for backwards compatibility with HDFS connector and
        // will be removed in future versions.
        CONFIG_DEF.define(
            HDFS_URL_CONFIG,
            Type.STRING,
            HDFS_URL_DEFAULT,
            Importance.HIGH,
            HDFS_URL_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.MEDIUM,
            HDFS_URL_DISPLAY
        );

        CONFIG_DEF.define(
            HADOOP_CONF_DIR_CONFIG,
            Type.STRING,
            HADOOP_CONF_DIR_DEFAULT,
            Importance.HIGH,
            HADOOP_CONF_DIR_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.MEDIUM,
            HADOOP_CONF_DIR_DISPLAY
        );

        CONFIG_DEF.define(
            HADOOP_HOME_CONFIG,
            Type.STRING,
            HADOOP_HOME_DEFAULT,
            Importance.HIGH,
            HADOOP_HOME_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            HADOOP_HOME_DISPLAY
        );
        CONFIG_DEF.define(
            STORAGE_ROOT_PATH,
            Type.STRING,
            STORAGE_ROOT_PATH_DEFAULT,
            Importance.HIGH,
            STORAGE_ROOT_PATH_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            STORAGE_ROOT_PATH_DISPLAY
        );
        CONFIG_DEF.define(
            STORAGE_MODE,
            Type.STRING,
            STORAGE_MODE_DEFAULT,
            Importance.HIGH,
            STORAGE_MODE_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            STORAGE_MODE_DISPLAY
        );
        CONFIG_DEF.define(
            HIVE_INTEGRATION_MODE,
            Type.STRING,
            HIVE_INTEGRATION_MODE_DEFAULT,
            Importance.HIGH,
            HIVE_INTEGRATION_MODE_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            HIVE_INTEGRATION_MODE_DISPLAY
        );
        CONFIG_DEF.define(
            HIVE_SCHEMA_CHANGE_SUPPORT,
            Type.BOOLEAN,
            HIVE_SCHEMA_CHANGE_SUPPORT_DEFAULT,
            Importance.HIGH,
            HIVE_SCHEMA_CHANGE_SUPPORT_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            HIVE_SCHEMA_CHANGE_SUPPORT_DISPLAY
        );
        CONFIG_DEF.define(
            STORAGE_FORMAT,
            Type.STRING,
            STORAGE_FORMAT_DEFAULT,
            Importance.HIGH,
            STORAGE_FORMAT_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            STORAGE_FORMAT_DISPLAY
        );
        CONFIG_DEF.define(
            STORAGE_FORMAT_TEXT_SEPARATOR,
            Type.STRING,
            STORAGE_FORMAT_TEXT_SEPARATOR_DEFAULT,
            Importance.HIGH,
            STORAGE_FORMAT_TEXT_SEPARATOR_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            STORAGE_FORMAT_TEXT_SEPARATOR_DISPLAY
        );
        CONFIG_DEF.define(
            ROTATION_POLICY,
            Type.STRING,
            ROTATION_POLICY_DEFAULT,
            Importance.HIGH,
            ROTATION_POLICY_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            ROTATION_POLICY_DISPLAY
        );

        CONFIG_DEF.define(
            ROTATION_POLICY_FILE_SIZE,
            Type.LONG,
            ROTATION_POLICY_FILE_SIZE_DEFAULT,
            Importance.HIGH,
            ROTATION_POLICY_FILE_SIZE_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            ROTATION_POLICY_BLOCK_SIZE_DISPLAY
        );

        CONFIG_DEF.define(
            ROTATION_POLICY_RECORD_SIZE,
            Type.LONG,
            ROTATION_POLICY_RECORD_SIZE_DEFAULT,
            Importance.HIGH,
            ROTATION_POLICY_RECORD_SIZE_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            ROTATION_POLICY_RECORD_SIZE_DISPLAY
        );

        CONFIG_DEF.define(
            HADOOP_USER,
            Type.STRING,
            HADOOP_USER_DEFAULT,
            Importance.HIGH,
            HADOOP_USER_DOC,
            hdfsGroup,
            ++orderInHdfsGroup,
            Width.SHORT,
            HADOOP_USER_DISPLAY
        );

        final String securityGroup = "Security";
        int orderInSecurityGroup = 0;
        // Define Security configuration group
        CONFIG_DEF.define(
            HDFS_AUTHENTICATION_KERBEROS_CONFIG,
            Type.BOOLEAN,
            HDFS_AUTHENTICATION_KERBEROS_DEFAULT,
            Importance.HIGH,
            HDFS_AUTHENTICATION_KERBEROS_DOC,
            securityGroup,
            ++orderInSecurityGroup,
            Width.SHORT,
            HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
            Arrays.asList(
                CONNECT_HDFS_PRINCIPAL_CONFIG,
                CONNECT_HDFS_KEYTAB_CONFIG,
                HDFS_NAMENODE_PRINCIPAL_CONFIG,
                KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG
            )
        );

        CONFIG_DEF.define(
            CONNECT_HDFS_PRINCIPAL_CONFIG,
            Type.STRING,
            CONNECT_HDFS_PRINCIPAL_DEFAULT,
            Importance.HIGH,
            CONNECT_HDFS_PRINCIPAL_DOC,
            securityGroup,
            ++orderInSecurityGroup,
            Width.MEDIUM,
            CONNECT_HDFS_PRINCIPAL_DISPLAY,
            HDFS_AUTHENTICATION_KERBEROS_DEPENDENTS_RECOMMENDER
        );

        CONFIG_DEF.define(
            CONNECT_HDFS_KEYTAB_CONFIG,
            Type.STRING,
            CONNECT_HDFS_KEYTAB_DEFAULT,
            Importance.HIGH,
            CONNECT_HDFS_KEYTAB_DOC,
            securityGroup,
            ++orderInSecurityGroup,
            Width.MEDIUM,
            CONNECT_HDFS_KEYTAB_DISPLAY,
            HDFS_AUTHENTICATION_KERBEROS_DEPENDENTS_RECOMMENDER
        );

        CONFIG_DEF.define(
            HDFS_NAMENODE_PRINCIPAL_CONFIG,
            Type.STRING,
            HDFS_NAMENODE_PRINCIPAL_DEFAULT,
            Importance.HIGH,
            HDFS_NAMENODE_PRINCIPAL_DOC,
            securityGroup,
            ++orderInSecurityGroup,
            Width.MEDIUM,
            HDFS_NAMENODE_PRINCIPAL_DISPLAY,
            HDFS_AUTHENTICATION_KERBEROS_DEPENDENTS_RECOMMENDER
        );

        CONFIG_DEF.define(
            KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG,
            Type.LONG,
            KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT,
            Importance.LOW,
            KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
            securityGroup,
            ++orderInSecurityGroup,
            Width.SHORT,
            KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY,
            HDFS_AUTHENTICATION_KERBEROS_DEPENDENTS_RECOMMENDER
        );
        // Put the storage group(s) last ...
        ConfigDef storageConfigDef = StorageSinkConnectorConfig.newConfigDef(
        );
        for (ConfigDef.ConfigKey key : storageConfigDef.configKeys().values()) {
            CONFIG_DEF.define(key);
        }
    }

    private final String url;

    private final StorageCommonConfig commonConfig;

    private final HiveConfig hiveConfig;

    private final PartitionerConfig partitionerConfig;

    private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();

    private final Set<AbstractConfig> allConfigs = new HashSet<>();

    private Configuration hadoopConfig;

    private int taskId;

    private boolean initialized;

    private String database;

    private String table;

    public HdfsSinkConfig(final Map<String, String> props) {
        super(addDefaults(props));
        initialized = true;

        ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
        commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
        hiveConfig = new HiveConfig(originalsStrings());
        ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER);
        partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());
        taskId = Integer.parseInt(props.getOrDefault(HdfsSinkConnector.TASK_ID_CONFIG_NAME, HdfsSinkConnector.DEFAULT_TASK_ID));
        addToGlobal(hiveConfig);
        addToGlobal(partitionerConfig);
        addToGlobal(commonConfig);
        addToGlobal(this);
        this.url = extractUrl();
        validateTimezone();
        // database and table
        parseTable();
        initHadoopConf();
    }

    private void initHadoopConf() {
        this.hadoopConfig = new Configuration();
        Map<String, Object> hdfsConfProps = originalsWithPrefix(HDFS_CONF_PREFIX);
        Preconditions.checkNotNull(hdfsConfProps);
        hdfsConfProps.forEach((k, v) -> hadoopConfig.set(k, String.valueOf(v)));
    }

    /**
     * Validate the timezone with the rotate.schedule.interval.ms config,
     * these need validation before use in the TopicPartitionWriter.
     */
    private void validateTimezone() {
        String timezone = getString(PartitionerConfig.TIMEZONE_CONFIG);
        long rotateScheduleIntervalMs = getLong(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
        if (rotateScheduleIntervalMs > 0 && timezone.isEmpty()) {
            throw new ConfigException(
                String.format(
                    "%s configuration must be set when using %s",
                    PartitionerConfig.TIMEZONE_CONFIG,
                    ROTATE_SCHEDULE_INTERVAL_MS_CONFIG
                )
            );
        }
    }

    /**
     * Add default value.
     * @param  props properties map
     * @return properties map with default value
     */
    public static Map<String, String> addDefaults(final Map<String, String> props) {
        ConcurrentMap<String, String> propsCopy = new ConcurrentHashMap<>(props);
        propsCopy.putIfAbsent(StorageCommonConfig.STORAGE_CLASS_CONFIG, HdfsStorage.class.getName());
//        propsCopy.putIfAbsent(HdfsSinkConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
        return propsCopy;
    }

    private void addToGlobal(final AbstractConfig config) {
        allConfigs.add(config);
        addConfig(config.values(), (ComposableConfig) config);
    }

    private void addConfig(final Map<String, ?> parsedProps, final ComposableConfig config) {
        for (String key : parsedProps.keySet()) {
            propertyToConfig.put(key, config);
        }
    }

    /**
     * Returns the url property. Preference is given to property <code>store.url</code> over
     * <code>hdfs.url</code> because <code>hdfs.url</code> is deprecated.
     *
     * @return String url for HDFS
     */
    private String extractUrl() {
        String storageUrl = getString(StorageCommonConfig.STORE_URL_CONFIG);
        if (!Strings.isNullOrEmpty(storageUrl)) {

            return storageUrl;
        }

        String hdfsUrl = getString(HDFS_URL_CONFIG);
        if (!Strings.isNullOrEmpty(hdfsUrl)) {
            return hdfsUrl;
        }

        throw new ConfigException(
            String.format("Configuration %s cannot be empty.", StorageCommonConfig.STORE_URL_CONFIG)
        );
    }

    /**
     * Get HDFS principal config.
     * @return principal
     */
    public String connectHdfsPrincipal() {
        return getString(CONNECT_HDFS_PRINCIPAL_CONFIG);
    }

    /**
     * Get hdfs keytab.
     * @return hdfs keytab
     */
    public String connectHdfsKeytab() {
        return getString(CONNECT_HDFS_KEYTAB_CONFIG);
    }

    /**
     * Get hadoop config root dir.
     * @return hdfs keytab
     */
    public String hadoopConfDir() {
        return getString(HADOOP_CONF_DIR_CONFIG);
    }

    /**
     * Get hadoop config home.
     * @return hadoop home config
     */
    public String hadoopHome() {
        return getString(HADOOP_HOME_CONFIG);
    }

    /**
     * Get nameNode principal config.
     * @return principal
     */
    public String hdfsNamenodePrincipal() {
        return getString(HDFS_NAMENODE_PRINCIPAL_CONFIG);
    }

    /**
     * Get kerberosAuthentication config switch.
     * @return boolean
     */
    public boolean kerberosAuthentication() {
        return getBoolean(HDFS_AUTHENTICATION_KERBEROS_CONFIG);
    }

    /**
     * Get kerberos ticket period.
     * @return boolean
     */
    public long kerberosTicketRenewPeriodMs() {
        return getLong(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG);
    }

    /**
     * Get HDFS name.
     * @return HDFS name
     */
    public String name() {
        return originalsStrings().getOrDefault("name", "HDFS-sink");
    }

    /**
     * Get HDFS url.
     * @return HDFS url
     */
    public String url() {
        return url;
    }

    @Override
    public Object get(final String key) {
        // 兼容父类的掉用
        if (!initialized) {
            return super.get(key);
        }

        ComposableConfig config = propertyToConfig.get(key);
        if (config == null) {
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        }
        return config == this ? super.get(key) : config.get(key);
    }

    /**
     * Get hadoop configuration.
     * @return hadoop config
     */
    public Configuration getHadoopConfiguration() {
        return hadoopConfig;
    }

    /**
     * All configuration be plain.
     * @return plain map
     */
    public Map<String, ?> plainValues() {
        Map<String, Object> map = new HashMap<>();
        for (AbstractConfig config : allConfigs) {
            map.putAll(config.values());
        }
        // Include any additional properties not defined by the ConfigDef
        // that custom partitioners might need
        Map<String, ?> originals = originals();
        for (String originalKey : originals.keySet()) {
            if (!map.containsKey(originalKey)) {
                map.put(originalKey, originals.get(originalKey));
            }
        }

        return map;
    }

    /**
     * Get connect config define.
     * @return config  key  define.
     */
    public static ConfigDef getConfig() {
        // Define the names of the configurations we're going to override
        Set<String> skip = new HashSet<>();
        skip.add(StorageCommonConfig.STORAGE_CLASS_CONFIG);
        // Order added is important, so that group order is maintained
        ConfigDef visible = new ConfigDef();
        // TODO 暂时先这样解决，先保持兼容
        addAllConfigKeys(visible, CONFIG_DEF, skip);
        addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
        addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);
        addAllConfigKeys(visible, HiveConfig.getConfig(), skip);

        // Add the overridden configurations
        visible.define(
            StorageCommonConfig.STORAGE_CLASS_CONFIG,
            Type.CLASS,
            HdfsStorage.class.getName(),
            Importance.HIGH,
            StorageCommonConfig.STORAGE_CLASS_DOC,
            "Storage",
            1,
            Width.NONE,
            StorageCommonConfig.STORAGE_CLASS_DISPLAY,
            STORAGE_CLASS_RECOMMENDER
        );
        return visible;
    }

    private static void addAllConfigKeys(final ConfigDef container, final ConfigDef other, final Set<String> skip) {
        for (ConfigDef.ConfigKey key : other.configKeys().values()) {
            if (skip != null && !skip.contains(key.name)) {
                container.define(key);
            }
        }
    }

    /**
     * Get task id.
     * @return task id
     */
    public int getTaskId() {
        return taskId;
    }

    private void parseTable() {
        List<String> tables = getList(SinkConfig.DESTINATIONS);
        String tableName = tables.get(0);
        String[] confTableArr = tableName.split(HdfsSinkConstants.DB_SEPARATOR_REG);
        if (null == confTableArr
            || confTableArr.length <= 0
            || Strings.isNullOrEmpty(confTableArr[0])
            || Strings.isNullOrEmpty(confTableArr[1])) {
            throw new ConfigException(String.format("invalided tableName,value: %s", tableName));
        }
        this.database = confTableArr[0];
        this.table = confTableArr[1];
    }

    /**
     * Get database.
     * @return database name
     */
    public String database() {
        return database;
    }

    /**
     * Get table.
     * @return table name
     */
    public String table() {
        return table;
    }

    private static class BooleanParentRecommender implements ConfigDef.Recommender {

        private String parentConfigName;

        BooleanParentRecommender(final String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return (Boolean) connectorConfigs.get(parentConfigName);
        }
    }
}
