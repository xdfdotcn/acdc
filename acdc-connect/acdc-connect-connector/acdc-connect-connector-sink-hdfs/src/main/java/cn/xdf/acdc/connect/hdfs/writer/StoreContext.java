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

package cn.xdf.acdc.connect.hdfs.writer;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.SchemaReader;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroFileReader;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.metadata.HiveMetaReader;
import cn.xdf.acdc.connect.hdfs.format.orc.OrcFileReader;
import cn.xdf.acdc.connect.hdfs.format.orc.OrcRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.parquet.ParquetFileReader;
import cn.xdf.acdc.connect.hdfs.format.parquet.ParquetRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.text.TextRecordAppendWriterProvider;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.initialize.DefaultStoreConfigFactory;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.initialize.HiveMetaStoreConfigFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StorageMode;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerFactory;
import cn.xdf.acdc.connect.hdfs.rotation.FileSizeRotationPolicy;
import cn.xdf.acdc.connect.hdfs.rotation.RecordSizeRotationPolicy;
import cn.xdf.acdc.connect.hdfs.rotation.RotationPolicy;
import cn.xdf.acdc.connect.hdfs.rotation.RotationPolicyType;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.ConfigException;

@Getter
@Setter
@Accessors(chain = true)
public class StoreContext {

    private static final int THREAD_POOL_CORE_SIZE = 1;

    private static final int THREAD_POOL_MAX_SIZE = 1;

    private static final long THREAD_POOL_ALIVE_TIME = 0;

    private static final int THREAD_POOL_WORK_QUEUE_SIZE = 100;

    private HdfsSinkConfig hdfsSinkConfig;

    private HiveIntegrationMode hiveIntegrationMode;

    private StorageMode storageMode;

    private StoreConfig storeConfig;

    private HdfsFileOperator fileOperator;

    private SchemaReader schemaReader;

    private RecordWriterProvider recordWriterProvider;

    private Partitioner partitioner;

    private HiveMetaStore hiveMetaStore;

    private ExecutorService executor;

    private RotationPolicy rotationPolicy;

    private HiveMetaRestorer hiveMetaRestorer;

    /**
     * Build store context.
     * @param hdfsSinkConf  config
     * @return store context
     * @throws IOException exception on create store component collection
     */
    public static StoreContext buildContext(final HdfsSinkConfig hdfsSinkConf) throws IOException {
        HiveMetaStore hiveMetaStore = createMetaStore(hdfsSinkConf, new Configuration());
        return buildContext(hdfsSinkConf, hiveMetaStore);
    }

    /**
     * Build store context.
     * @param hdfsSinkConf  config
     * @param hadoopConf  hadoop config
     * @return store context
     * @throws IOException exception on create store component collection
     */
    public static StoreContext buildContext(final HdfsSinkConfig hdfsSinkConf, final Configuration hadoopConf) throws IOException {
        HiveMetaStore hiveMetaStore = createMetaStore(hdfsSinkConf, hadoopConf);
        return buildContext(hdfsSinkConf, hiveMetaStore);
    }

    /**
     * Build store context.
     * @param hdfsSinkConf  config
     * @param hiveMetaStore  hive metastore
     * @return store context
     * @throws IOException exception on create store component collection
     */
    public static StoreContext buildContext(
        final HdfsSinkConfig hdfsSinkConf,
        final HiveMetaStore hiveMetaStore
    ) throws IOException {

        StoreConfig storeConf = createStoreConfig(hdfsSinkConf, hiveMetaStore);

        Partitioner partitioner = createPartitioner(hdfsSinkConf);

        HdfsFileOperator fileOperator = createHdfsFileOperator(hdfsSinkConf, storeConf);

        SchemaReader schemaReader = createSchemaReader(hdfsSinkConf, storeConf, fileOperator, hiveMetaStore);

        RecordWriterProvider writerProvider = createRecordWriterProvider(hdfsSinkConf, storeConf, fileOperator);

        RotationPolicy rotationPolicy = createRotationPolicy(hdfsSinkConf);

        HiveMetaRestorer hiveMetaRestorer = createHiveTableRestorer(
            hdfsSinkConf,
            storeConf,
            hiveMetaStore,
            schemaReader,
            partitioner
        );

        return new StoreContext()
            .setHiveIntegrationMode(
                HiveIntegrationMode
                    .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE))
            )
            .setStorageMode(
                StorageMode.valueOf(hdfsSinkConf.getString(HdfsSinkConfig.STORAGE_MODE))
            )
            .setHiveMetaStore(hiveMetaStore)
            .setStoreConfig(storeConf)
            .setPartitioner(partitioner)
            .setFileOperator(fileOperator)
            .setSchemaReader(schemaReader)
            .setRecordWriterProvider(writerProvider)
            .setRotationPolicy(rotationPolicy)
            .setHiveMetaRestorer(hiveMetaRestorer)
            .setHdfsSinkConfig(hdfsSinkConf);
    }

    private static Partitioner createPartitioner(final HdfsSinkConfig config) {
        return PartitionerFactory.createPartitioner(config);
    }

    private static HiveMetaRestorer createHiveTableRestorer(
        final HdfsSinkConfig hdfsSinkConf,
        final StoreConfig storeConf,
        final HiveMetaStore hiveMetaStore,
        final SchemaReader schemaReader,
        final Partitioner partitioner
    ) {
        Preconditions.checkNotNull(partitioner, "Partitioner can not be null.");
        Preconditions.checkNotNull(partitioner, "Executor can not be null.");
        Preconditions.checkNotNull(partitioner, "SchemaReader can not be null.");

        return new HiveMetaRestorer(
            hdfsSinkConf,
            storeConf,
            hiveMetaStore,
            schemaReader,
            partitioner
        );
    }

    private static HiveMetaStore createMetaStore(final HdfsSinkConfig hdfsSinkConf, final Configuration hadoopConf) {
        HiveIntegrationMode hiveIntegrationMode = HiveIntegrationMode
            .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE));

        return hiveIntegrationMode.isIntegrationHive()
            ? new HiveMetaStore(hadoopConf, hdfsSinkConf)
            : HiveMetaStore.EMPTY_META_STORE;
    }

    private static StoreConfig createStoreConfig(final HdfsSinkConfig hdfsSinkConf, final HiveMetaStore hiveMetaStore) {
        HiveIntegrationMode hiveIntegrationMode = HiveIntegrationMode
            .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE));

        switch (hiveIntegrationMode) {
            case WITH_HIVE_META_DATA:
                Objects.requireNonNull(hiveMetaStore);
                return new HiveMetaStoreConfigFactory(hdfsSinkConf, hiveMetaStore).createStoreConfig();
            case AUTO_CREATE_EXTERNAL_TABLE:
            case NONE:
                return new DefaultStoreConfigFactory(hdfsSinkConf).createStoreConfig();
            default:
                throw new ConfigException("UNKNOWN HiveIntegrationMode :" + hiveIntegrationMode);
        }
    }

    private static HdfsFileOperator createHdfsFileOperator(
        final HdfsSinkConfig hdfsSinkConf,
        final StoreConfig storeConf
    ) throws IOException {
        HdfsStorage storage = new HdfsStorage(hdfsSinkConf, hdfsSinkConf.url());
        HdfsFileOperator fileOperator = new HdfsFileOperator(storage, storeConf, hdfsSinkConf);
        return fileOperator;
    }

    private static SchemaReader createSchemaReader(
        final HdfsSinkConfig hdfsSinkConf,
        final StoreConfig storeConf,
        final HdfsFileOperator fileOperator,
        final HiveMetaStore hiveMetaStore
    ) {
        HiveIntegrationMode hiveIntegrationMode = HiveIntegrationMode
            .valueOf(hdfsSinkConf.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE));

        if (hiveIntegrationMode.isWithHiveMetaData()) {
            Preconditions.checkNotNull(hiveMetaStore);
            Preconditions.checkNotNull(hiveMetaStore, "Integration hive, metastore not be null");

            return new HiveMetaReader(
                hdfsSinkConf,
                storeConf,
                fileOperator,
                hiveMetaStore
            );
        }

        Format format = storeConf.format();
        switch (format) {
            case ORC:
                return new OrcFileReader(hdfsSinkConf, storeConf, fileOperator);
            case AVRO:
                return new AvroFileReader(hdfsSinkConf, storeConf, fileOperator);
            case PARQUET:
                return new ParquetFileReader(hdfsSinkConf, storeConf, fileOperator);
            case TEXT:
                return new HiveMetaReader(hdfsSinkConf, storeConf, fileOperator, hiveMetaStore);
            default:
                throw new ConfigException("UNKNOWN Format type:" + storeConf.format());
        }
    }

    private static RotationPolicy createRotationPolicy(final HdfsSinkConfig hdfsSinkConf) {
        RotationPolicyType type = RotationPolicyType.valueOf(hdfsSinkConf.getString(HdfsSinkConfig.ROTATION_POLICY));

        switch (type) {
            case RECORD_SIZE:
                return new RecordSizeRotationPolicy(hdfsSinkConf);
            case FILE_SIZE:
                return new FileSizeRotationPolicy(hdfsSinkConf);
            default:
                throw new ConfigException("UNKNOWN: " + type);
        }
    }

    private static RecordWriterProvider createRecordWriterProvider(
        final HdfsSinkConfig hdfsSinkConf,
        final StoreConfig storeConf,
        final HdfsFileOperator fileOperator
    ) {
        Format format = storeConf.format();
        switch (format) {
            case ORC:
                return new OrcRecordWriterProvider(hdfsSinkConf);
            case AVRO:
                return new AvroRecordWriterProvider(fileOperator, hdfsSinkConf);
            case PARQUET:
                return new ParquetRecordWriterProvider(fileOperator, hdfsSinkConf);
            case TEXT:
                return new TextRecordAppendWriterProvider(fileOperator, storeConf);
            default:
                throw new ConfigException("UNKNOWN Format type:" + storeConf.format());
        }
    }
}
