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
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroFileReader;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.metadata.HiveMetaReader;
import cn.xdf.acdc.connect.hdfs.format.orc.OrcFileReader;
import cn.xdf.acdc.connect.hdfs.format.orc.OrcRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.parquet.ParquetFileReader;
import cn.xdf.acdc.connect.hdfs.format.parquet.ParquetRecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.text.TextRecordAppendWriterProvider;
import cn.xdf.acdc.connect.hdfs.hive.HiveConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveTextTestBase;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.initialize.StorageMode;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 {@link HdfsWriterCoordinator}.
 */
public class HdfsWriterCoordinatorTest extends HiveTextTestBase {

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "src/test/resources/conf");
        // store mode
        props.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
        // partitioner
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        return props;
    }

    @Test
    public void testSystemPropertyWhenConfigureHadoopUser() throws Exception {
        setUp();
        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.WITH_HIVE_META_DATA.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.TEXT.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.AT_LEAST_ONCE.name());
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(HdfsSinkConfig.HADOOP_USER, "test");
        new HdfsWriterCoordinator(new HdfsSinkConfig(props), context);
        assertEquals("test", System.getProperty("HADOOP_USER_NAME"));

        props = createProps();
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.WITH_HIVE_META_DATA.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.TEXT.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.AT_LEAST_ONCE.name());
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        new HdfsWriterCoordinator(new HdfsSinkConfig(props), context);
        assertEquals("hive", System.getProperty("HADOOP_USER_NAME"));
    }

    @Test
    public void testAtLeastOnceTextFormat() throws Exception {
        setUp();
        Map<String, String> props = createProps();
        // hive integration mode
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.WITH_HIVE_META_DATA.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.TEXT.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.AT_LEAST_ONCE.name());
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(HdfsSinkConfig.HADOOP_USER, "test");
        HdfsSinkConfig hdfsSinkConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator writerCoordinator = new HdfsWriterCoordinator(hdfsSinkConfig, context);
        StoreContext storeContext = writerCoordinator.getStoreContext();
        assertTrue(storeContext.getRecordWriterProvider() instanceof TextRecordAppendWriterProvider);
        assertTrue(storeContext.getSchemaReader() instanceof HiveMetaReader);
        Partitioner partitioner = storeContext.getPartitioner();
        List<SinkRecord> sinkRecords = createSinkRecords(1);
        writerCoordinator.write(sinkRecords);
        AtLeastOnceTopicPartitionWriter tpWriter =
            (AtLeastOnceTopicPartitionWriter) writerCoordinator.getBucketWriter(TOPIC_PARTITION);
        RecordWriter recordWriter = tpWriter.getEncodePartitionWriters().get(partitioner.encodePartition(null));
        long fileSize = recordWriter.fileSize();
        assertTrue(recordWriter.fileSize() != 0);
        writerCoordinator.close();

        writerCoordinator = new HdfsWriterCoordinator(hdfsSinkConfig, context);
        writerCoordinator.write(sinkRecords);
        tpWriter =
            (AtLeastOnceTopicPartitionWriter) writerCoordinator.getBucketWriter(TOPIC_PARTITION);
        recordWriter = tpWriter.getEncodePartitionWriters().get(partitioner.encodePartition(null));
        assertTrue(recordWriter.fileSize() > fileSize);
        tpWriter.close();
    }

    @Test
    public void testExactlyOnceOrcFormat() throws Exception {
        setUp();
        Map<String, String> props = createProps();
        // hive integration mode
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.AUTO_CREATE_EXTERNAL_TABLE.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.ORC.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.EXACTLY_ONCE.name());
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "1");
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        HdfsSinkConfig hdfsSinkConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator writerCoordinator = new HdfsWriterCoordinator(hdfsSinkConfig, context);
        StoreContext storeContext = writerCoordinator.getStoreContext();
        assertTrue(storeContext.getRecordWriterProvider() instanceof OrcRecordWriterProvider);
        assertTrue(storeContext.getSchemaReader() instanceof OrcFileReader);
        List<SinkRecord> sinkRecords = createSinkRecords(1);
        writerCoordinator.write(sinkRecords);
        writerCoordinator.close();
    }

    @Test
    public void testExactlyOnceParquetFormat() throws Exception {
        setUp();
        Map<String, String> props = createProps();
        // hive integration mode
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.AUTO_CREATE_EXTERNAL_TABLE.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.PARQUET.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.EXACTLY_ONCE.name());
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "1");
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        HdfsSinkConfig hdfsSinkConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator writerCoordinator = new HdfsWriterCoordinator(hdfsSinkConfig, context);
        StoreContext storeContext = writerCoordinator.getStoreContext();
        StoreConfig storeConfig = storeContext.getStoreConfig();
        storeContext.getHiveMetaStore().dropTable(storeConfig.database(), storeConfig.table());
        assertTrue(storeContext.getRecordWriterProvider() instanceof ParquetRecordWriterProvider);
        assertTrue(storeContext.getSchemaReader() instanceof ParquetFileReader);
        List<SinkRecord> sinkRecords = createSinkRecords(1);
        writerCoordinator.write(sinkRecords);
        writerCoordinator.close();
    }

    @Test
    public void testExactlyOnceAvroFormat() throws Exception {
        setUp();
        Map<String, String> props = createProps();
        // hive integration mode
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.AUTO_CREATE_EXTERNAL_TABLE.name());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.AVRO.name());
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.EXACTLY_ONCE.name());
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "1");
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        HdfsSinkConfig hdfsSinkConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator writerCoordinator = new HdfsWriterCoordinator(hdfsSinkConfig, context);
        StoreContext storeContext = writerCoordinator.getStoreContext();
        StoreConfig storeConfig = storeContext.getStoreConfig();
        storeContext.getHiveMetaStore().dropTable(storeConfig.database(), storeConfig.table());
        assertTrue(storeContext.getRecordWriterProvider() instanceof AvroRecordWriterProvider);
        assertTrue(storeContext.getSchemaReader() instanceof AvroFileReader);
        List<SinkRecord> sinkRecords = createSinkRecords(1);
        writerCoordinator.write(sinkRecords);
        writerCoordinator.close();
    }
}
