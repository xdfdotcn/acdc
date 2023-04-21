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

import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.initialize.StorageMode;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.storage.schema.StorageSchemaCompatibility;
import cn.xdf.acdc.connect.hdfs.writer.AtLeastOnceTopicPartitionWriter;
import cn.xdf.acdc.connect.hdfs.writer.ExactlyOnceTopicPartitionWriter;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import cn.xdf.acdc.connect.hdfs.writer.TopicPartitionWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class HdfsWriterCoordinator {

    private static final Time SYSTEM_TIME = new SystemTime();

    private static final String HADOOP_ENV_USER = "HADOOP_USER_NAME";

    private static final String HADOOP_ENV_HOME_DIR = "hadoop.home.dir";

    private final Time time;

    private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;

    /**
     * Thread Pool config.
     */
    private final HdfsSinkConfig hdfsSinkConf;

    private final SinkTaskContext context;

    private final StoreContext storeContext;

    private Thread ticketRenewThread;

    private volatile boolean isRunning;

    public HdfsWriterCoordinator(
            final HdfsSinkConfig hdfsSinkConfig,
            final SinkTaskContext context
    ) {
        this(hdfsSinkConfig, context, SYSTEM_TIME);

    }

    public HdfsWriterCoordinator(
            final HdfsSinkConfig hdfsSinkConfig,
            final SinkTaskContext context,
            final Time time
    ) {
        this.time = time;
        this.hdfsSinkConf = hdfsSinkConfig;
        this.context = context;
        this.topicPartitionWriters = new HashMap<>();
        Configuration configuration = getHadoopConf();
        initializeAuth(configuration);
        this.storeContext = initializeStoreContext(configuration, this.hdfsSinkConf);
//        initializeStorageDir();
        initializeTpWriters(context.assignment());
    }

    private Configuration getHadoopConf() {
        System.setProperty(HADOOP_ENV_USER, hdfsSinkConf.getString(HdfsSinkConfig.HADOOP_USER));
        System.setProperty(HADOOP_ENV_HOME_DIR, hdfsSinkConf.hadoopHome());
        log.info("Hadoop configuration directory {}", hdfsSinkConf.hadoopConfDir());
        Configuration hadoopConfiguration = hdfsSinkConf.getHadoopConfiguration();
        if (!this.hdfsSinkConf.hadoopConfDir().equals("")) {
            hadoopConfiguration.addResource(new Path(hdfsSinkConf.hadoopConfDir() + "/core-site.xml"));
            hadoopConfiguration.addResource(new Path(hdfsSinkConf.hadoopConfDir() + "/hdfs-site.xml"));
        }
        return hadoopConfiguration;
    }

    private void initializeAuth(final Configuration hadoopConfiguration) {
        if (this.hdfsSinkConf.kerberosAuthentication()) {
            configureKerberosAuthentication(hadoopConfiguration);
        }
    }

    private void configureKerberosAuthentication(final Configuration hadoopConfiguration) {
        SecurityUtil.setAuthenticationMethod(
                UserGroupInformation.AuthenticationMethod.KERBEROS,
                hadoopConfiguration
        );

        if (this.hdfsSinkConf.connectHdfsPrincipal() == null
                || this.hdfsSinkConf.connectHdfsKeytab() == null) {
            throw new ConfigException(
                    "Hadoop is using Kerberos for authentication, you need to provide both a connect "
                            + "principal and the path to the keytab of the principal.");
        }

        hadoopConfiguration.set("hadoop.security.authentication", "kerberos");
        hadoopConfiguration.set("hadoop.security.authorization", "true");

        try {
            String hostname = InetAddress.getLocalHost().getCanonicalHostName();

            String namenodePrincipal = SecurityUtil.getServerPrincipal(
                    hdfsSinkConf.hdfsNamenodePrincipal(),
                    hostname
            );

            // namenode principal is needed for multi-node hadoop cluster
            if (hadoopConfiguration.get("dfs.namenode.kerberos.principal") == null) {
                hadoopConfiguration.set("dfs.namenode.kerberos.principal", namenodePrincipal);
            }
            log.info("Hadoop namenode principal: {}",
                    hadoopConfiguration.get("dfs.namenode.kerberos.principal"));

            UserGroupInformation.setConfiguration(hadoopConfiguration);
            // replace the _HOST specified in the principal config to the actual host
            String principal = SecurityUtil.getServerPrincipal(
                    hdfsSinkConf.connectHdfsPrincipal(),
                    hostname
            );
            UserGroupInformation.loginUserFromKeytab(principal, hdfsSinkConf.connectHdfsKeytab());
            final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            log.info("Login as: " + ugi.getUserName());

            this.isRunning = true;
            this.ticketRenewThread = new Thread(() -> renewKerberosTicket(ugi));
        } catch (UnknownHostException e) {
            throw new ConnectException(
                    String.format(
                            "Could not resolve local hostname for Kerberos authentication: %s",
                            e.getMessage()
                    ),
                    e
            );
        } catch (IOException e) {
            throw new ConnectException(
                    String.format("Could not authenticate with Kerberos: %s", e.getMessage()),
                    e
            );
        }

        log.info(
                "Starting the Kerberos ticket renew thread with period {} ms.",
                hdfsSinkConf.kerberosTicketRenewPeriodMs()
        );
        ticketRenewThread.start();
    }

    private void initializeTpWriters(final Set<TopicPartition> assignment) {
        for (TopicPartition tp : assignment) {
            TopicPartitionWriter tpWriter = createTpWriter(tp);
            topicPartitionWriters.put(tp, tpWriter);
        }
    }

    /**
     * Write batch record to hdfs.
     *
     * @param records need write records
     */
    public void write(final Collection<SinkRecord> records) {
        // add to queue
        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);
            topicPartitionWriters.get(tp).buffer(record);
        }
        // Each partitionWriter write record
        for (TopicPartition tp : topicPartitionWriters.keySet()) {
            topicPartitionWriters.get(tp).write();
        }
    }

    /**
     * Recover WAL log,offset from hdfs .
     *
     * @param tp kafka topic partition
     */
    public void recover(final TopicPartition tp) {
        topicPartitionWriters.get(tp).recover();
    }

    /**
     * Recover WAL log,offset from hdfs .
     *
     * @param assignment assigned topic partitions
     */
    public void recover(final Set<TopicPartition> assignment) {
        if (Objects.nonNull(assignment) && !assignment.isEmpty()) {
            List<TopicPartition> assignmentList = new ArrayList<>(assignment);
            for (TopicPartition tp : assignmentList) {
                topicPartitionWriters.get(tp).recover();
            }

            // 1. 目前 sink connector 实例 为 table 纬度, 每个 topicPartitionWriter 被分配到的 topic partition 都来自一个 topic,即同一个表
            // 2. 为了减少 hive 元数据访问,同步hive元数据的逻辑只获取其中一个 topic partition 处理即可
            syncHiveMetaData(assignmentList.get(0));
        }
    }

    /**
     * Sync hive create table ,or add partitions.
     *
     * @param topicPartition topic partition
     */
    public void syncHiveMetaData(final TopicPartition topicPartition) {
        this.storeContext.getHiveMetaRestorer().syncHiveMetaData(topicPartition);
    }

    /**
     * Assigned topic partitions and recover .
     *
     * @param partitions assigned topic partitions
     */
    public void open(final Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            TopicPartitionWriter topicPartitionWriter = createTpWriter(tp);
            topicPartitionWriters.put(tp, topicPartitionWriter);
            // We need to immediately start recovery to ensure we pause consumption of messages for the
            // assigned topics while we try to recover offsets and rewind.
            recover(tp);
        }
    }

    private TopicPartitionWriter createTpWriter(final TopicPartition tp) {
        StorageMode storageMode = storeContext.getStorageMode();
        switch (storageMode) {
            case EXACTLY_ONCE:
                return new ExactlyOnceTopicPartitionWriter(context, time, tp, storeContext);
            case AT_LEAST_ONCE:
                return new AtLeastOnceTopicPartitionWriter(context, tp, storeContext);
            default:
                throw new ConfigException("UNKNOWN Type:" + storageMode);
        }
    }

    /**
     * Close all Writer .
     */
    public void close() {
        // Close any writers we have. We may get assigned the same partitions and end up duplicating
        // some effort since we'll have to reprocess those messages. It may be possible to hold on to
        // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
        // more complex due to potential failures and network partitions. For example, we may get
        // this close, then miss a few generations of group membership, during which
        // data may have continued to be processed and we'd have to restart from the recovery stage,
        // make sure we apply the WAL, and only reuse the temp file if the starting offset is still
        // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
        for (TopicPartitionWriter writer : topicPartitionWriters.values()) {
            try {
                if (writer != null) {
                    // In some failure modes, the writer might not have been created for all assignments
                    writer.close();
                }
            } catch (ConnectException e) {
                log.warn("Unable to close writer for topic partition {}: ", writer.topicPartition(), e);
            }
        }
        topicPartitionWriters.clear();
    }

    /**
     * Stop the client.
     */
    public void stop() {
        this.storeContext.getFileOperator().storage().close();

        if (ticketRenewThread != null) {
            synchronized (this) {
                isRunning = false;
                this.notifyAll();
            }
        }
    }

    /**
     * By convention, the consumer stores the offset that corresponds to the next record to consume. To follow this convention, this methods returns each offset that is one more than the last offset
     * committed to HDFS.
     *
     * @return Map from TopicPartition to next offset after the most recently committed offset to HDFS
     */
    public Map<TopicPartition, Long> getCommittedOffsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        log.debug("Writer looking for last offsets for topic partitions {}",
                topicPartitionWriters.keySet()
        );
        for (TopicPartition tp : topicPartitionWriters.keySet()) {
            long committedOffset = topicPartitionWriters.get(tp).offset();
            log.debug("Writer found last offset {} for topic partition {}", committedOffset, tp);
            if (committedOffset >= 0) {
                offsets.put(tp, committedOffset);
            }
        }
        return offsets;
    }

    private void renewKerberosTicket(final UserGroupInformation ugi) {
        synchronized (HdfsWriterCoordinator.this) {
            while (isRunning) {
                try {
                    HdfsWriterCoordinator.this.wait(hdfsSinkConf.kerberosTicketRenewPeriodMs());
                    if (isRunning) {
                        log.debug(" "
                                + "Attempting re-LOGin from keytab for user: {}", ugi.getUserName());
                        ugi.reloginFromKeytab();
                    }
                } catch (IOException e) {
                    // We ignore this exception during reLOGin as each successful reLOGin gives
                    // additional 24 hours of authentication in the default config. In normal
                    // situations, the probability of failing reLOGin 24 times is low and if
                    // that happens, the task will fail eventually.
                    log.error("Error renewing the ticket", e);
                } catch (InterruptedException e) {
                    // ignored
                }
            }
        }
    }

    private void verifyConf() {
        StorageMode storageMode = StorageMode
                .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.STORAGE_MODE));

        Format format = Format
                .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.STORAGE_FORMAT));

        HiveIntegrationMode hiveIntegrationMode = HiveIntegrationMode
                .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE));

        StorageSchemaCompatibility compatibility = StorageSchemaCompatibility
                .getCompatibility(
                        hdfsSinkConf
                                .getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG)
                );

        checkTimeZoneConfig();

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(hdfsSinkConf.url()),
                "HDFS url must be set."
        );

        Preconditions.checkArgument(
                !(storageMode.isAtLeastOnce() && (format != Format.TEXT || !hiveIntegrationMode.isWithHiveMetaData())),
                "At least once writer only support TEXT format and Hive integration mode must be WITH_HIVE_META_DATA"
        );

        Preconditions.checkArgument(
                !(storageMode.isExactlyOnce() && hiveIntegrationMode.isWithHiveMetaData() && (format != Format.ORC || format != Format.PARQUET)),
                "Exactly once writer only support ORC,PARQUET format when Hive integration mode is WITH_HIVE_META_DATA "
        );

        Preconditions.checkArgument(
                !(hiveIntegrationMode.isAutoCreateTable() && compatibility == StorageSchemaCompatibility.NONE),
                "Hive Integration requires schema compatibility to be BACKWARD, FORWARD or FULL."
        );
    }

    private void checkTimeZoneConfig() {
        //check that timezone it setup correctly in case of scheduled rotation
        if (hdfsSinkConf.getLong(HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG) > 0) {
            String timeZoneString = hdfsSinkConf.getString(PartitionerConfig.TIMEZONE_CONFIG);
            if (Strings.isNullOrEmpty(timeZoneString)) {
                throw new ConfigException(PartitionerConfig.TIMEZONE_CONFIG,
                        timeZoneString, "Timezone cannot be empty when using scheduled file rotation."
                );
            }
            DateTimeZone.forID(timeZoneString);
        }
    }

    private StoreContext initializeStoreContext(final Configuration hadoopConf, final HdfsSinkConfig hdfsSinkConf) {
        try {
            verifyConf();
            StoreContext storeContext = StoreContext.buildContext(hdfsSinkConf, hadoopConf);
            log.info("Init store context complete context: {}", storeContext);
            return storeContext;
        } catch (IOException e) {
            log.warn("Init storage components exception error message: {}", e.getMessage(), e);
            throw new ConnectException(String.format("Init storage components exception: %s", e.getMessage()), e);
        }
    }

    /**
     * Finish processing a batch records ,should be flush disk.
     */
    public void commit() {
        for (TopicPartition tp : topicPartitionWriters.keySet()) {
            topicPartitionWriters.get(tp).commit();
        }
    }

    /**
     * Get partitioner .
     *
     * @return partitioner
     */
    public Partitioner getPartitioner() {
        return this.storeContext.getPartitioner();
    }

    /**
     * Get topicPartitionWriter.
     *
     * @param tp kafka topic partition
     * @return topicPartitionWriter
     */
    public TopicPartitionWriter getBucketWriter(final TopicPartition tp) {
        return topicPartitionWriters.get(tp);
    }

    /**
     * Get store context only or test use .
     *
     * @return StoreContext
     */
    public StoreContext getStoreContext() {
        return storeContext;
    }
}
