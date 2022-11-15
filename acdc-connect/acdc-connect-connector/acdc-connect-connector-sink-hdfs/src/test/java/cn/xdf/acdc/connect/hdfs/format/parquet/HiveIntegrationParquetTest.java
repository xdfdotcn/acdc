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

package cn.xdf.acdc.connect.hdfs.format.parquet;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.hive.HiveConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestUtils;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.DailyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.FieldPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeUtils;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class HiveIntegrationParquetTest extends HiveTestBase {

    private final Map<String, String> localProps = new HashMap<>();

    @Before
    public void beforeTest() {
//        localProps.put(HdfsSinkConfig.HIVE_TABLE_NAME_CONFIG, hiveTableNameConfig);
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConfig.SHUTDOWN_TIMEOUT_CONFIG, "10000");
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.PARQUET.name());
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, "AUTO_CREATE_EXTERNAL_TABLE");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FULL");
        props.putAll(localProps);
        return props;
    }

    @Test
    public void testSyncWithHiveParquet() throws Exception {
        setUp();
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

//        localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
        HdfsSinkConfig config = new HdfsSinkConfig(createProps());
        hdfsWriter = new HdfsWriterCoordinator(config, context);
        hdfsWriter.syncHiveMetaData();

        Schema schema = createSchema();
        Struct expectedRecord = createRecord(schema);
        List<String> expectedResult = new ArrayList<>();
        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : schema.fields()) {
            expectedColumnNames.add(field.name());
            expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
        }

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);
        List<String> actualColumnNames = new ArrayList<>();
        for (FieldSchema column : table.getSd().getCols()) {
            actualColumnNames.add(column.getName());
        }
        assertEquals(expectedColumnNames, actualColumnNames);

        List<String> expectedPartitions = Arrays.asList(partitionLocation(PARTITION));
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);

        assertEquals(expectedPartitions, partitions);

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    @Test
    public void testHiveIntegrationParquet() throws Exception {
//        localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
        setUp();

        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        Schema schema = createSchema();
        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);
        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : schema.fields()) {
            expectedColumnNames.add(field.name());
        }

        List<String> actualColumnNames = new ArrayList<>();
        for (FieldSchema column : table.getSd().getCols()) {
            actualColumnNames.add(column.getName());
        }
        assertEquals(expectedColumnNames, actualColumnNames);

        List<String> expectedPartitions = new ArrayList<>();
        String directory = "partition=" + String.valueOf(PARTITION);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join(directory).build().path()
        );
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);
        assertEquals(expectedPartitions, partitions);
    }

    @Test
    public void testHiveIntegrationFieldPartitionerParquet() throws Exception {
        int batchSize = 3;
        int batchNum = 3;
//        localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
        localProps.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
        setUp();
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, batchSize, batchNum);
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : schema.fields()) {
            expectedColumnNames.add(field.name());
        }
        Collections.sort(expectedColumnNames);

        List<String> actualColumnNames = new ArrayList<>();
        // getAllCols is needed to include columns used for partitioning in result
        for (FieldSchema column : table.getAllCols()) {
            actualColumnNames.add(column.getName());
        }
        Collections.sort(actualColumnNames);

        assertEquals(expectedColumnNames, actualColumnNames);

        List<String> partitionFieldNames = connectorConfig.getList(
            PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
        );
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        String partitionFieldName = partitionFieldNames.get(0);
        String directory1 = partitionFieldName + "=" + String.valueOf(16);
        String directory2 = partitionFieldName + "=" + String.valueOf(17);
        String directory3 = partitionFieldName + "=" + String.valueOf(18);

        List<String> expectedPartitions = new ArrayList<>();
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join(directory1).build().path()
        );
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join(directory2).build().path()
        );
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join(directory3).build().path()
        );
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);

        assertEquals(expectedPartitions, partitions);

        Struct sampleRecord = createRecord(schema, 16, 12.2f);
        List<List<String>> expectedResults = new ArrayList<>();
        for (int batch = 0; batch < batchNum; ++batch) {
            int intForBatch = sampleRecord.getInt32("int") + batch;
            float floatForBatch = sampleRecord.getFloat32("float") + (float) batch;
            double doubleForBatch = sampleRecord.getFloat64("double") + (double) batch;
            for (int row = 0; row < batchSize; ++row) {
                // the partition field as column is last
                List<String> result = new ArrayList<>(
                    Arrays.asList("true", String.valueOf(intForBatch), String.valueOf(floatForBatch),
                        String.valueOf(doubleForBatch), String.valueOf(intForBatch)));
                expectedResults.add(result);
            }
        }

        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
        );
        String[] rows = result.split("\n");
        assertEquals(batchNum * batchSize, rows.length);
        for (int i = 0; i < rows.length; ++i) {
            String[] parts = HiveTestUtils.parseOutput(rows[i]);
            int j = 0;
            for (String expectedValue : expectedResults.get(i)) {
                assertEquals(expectedValue, parts[j++]);
            }
        }
    }

    @Test
    public void testHiveIntegrationFieldPartitionerParquetMultiple() throws Exception {
        localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
        localProps.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "country,state");
        setUp();
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        Schema schema = SchemaBuilder.struct()
            .field("count", Schema.INT64_SCHEMA)
            .field("country", Schema.STRING_SCHEMA)
            .field("state", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        List<Struct> records = Arrays.asList(
            new Struct(schema)
                .put("count", 1L)
                .put("country", "us")
                .put("state", "tx"),
            new Struct(schema)
                .put("count", 1L)
                .put("country", "us")
                .put("state", "ca"),
            new Struct(schema)
                .put("count", 1L)
                .put("country", "mx")
                .put("state", null)
        );
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : schema.fields()) {
            expectedColumnNames.add(field.name());
        }
        Collections.sort(expectedColumnNames);

        List<String> actualColumnNames = new ArrayList<>();
        // getAllCols is needed to include columns used for partitioning in result
        for (FieldSchema column : table.getAllCols()) {
            actualColumnNames.add(column.getName());
        }
        Collections.sort(actualColumnNames);

        assertEquals(expectedColumnNames, actualColumnNames);

        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        List<String> expectedPartitions = new ArrayList<>();
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join("/country=mx/state=null").build().path()
        );
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join("/country=us/state=ca").build().path()
        );
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join("/country=us/state=tx").build().path()
        );
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);

        assertEquals(expectedPartitions, partitions);

        List<List<String>> expectedResults = Arrays.asList(
            Arrays.asList("1", "mx", "null"),
            Arrays.asList("1", "us", "ca"),
            Arrays.asList("1", "us", "tx")
        );

        String result = HiveTestUtils.runHive(
            hiveExec, "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
        );
        String[] rows = result.split("\n");
        assertEquals(expectedResults.size(), rows.length);
        for (int i = 0; i < rows.length; ++i) {
            String[] parts = HiveTestUtils.parseOutput(rows[i]);
            int j = 0;
            for (String expectedValue : expectedResults.get(i)) {
                assertEquals(expectedValue, parts[j++]);
            }
        }
    }

    @Test
    public void testHiveIntegrationTimeBasedPartitionerParquet() throws Exception {
        localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
        setUp();
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : schema.fields()) {
            expectedColumnNames.add(field.name());
        }

        List<String> actualColumnNames = new ArrayList<>();
        for (FieldSchema column : table.getSd().getCols()) {
            actualColumnNames.add(column.getName());
        }
        assertEquals(expectedColumnNames, actualColumnNames);

        String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd";
        DateTime dateTime = DateTime.now(DateTimeZone.forID("America/Los_Angeles"));
        String encodedPartition = TimeUtils
            .encodeTimestamp(TimeUnit.HOURS.toMillis(24), pathFormat, "America/Los_Angeles",
                dateTime.getMillis());
        String directory = encodedPartition;
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        List<String> expectedPartitions = new ArrayList<>();
        expectedPartitions.add(
            FilePath.of(storeConfig.tablePath())
                .join(directory).build().path()
        );
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);
        assertEquals(expectedPartitions, partitions);

        ArrayList<String> partitionFields = new ArrayList<>();
        String[] groups = encodedPartition.split("/");
        for (String group : groups) {
            String field = group.split("=")[1];
            partitionFields.add(field);
        }

        List<List<String>> expectedResults = new ArrayList<>();
        for (int j = 0; j < 3; ++j) {
            for (int i = 0; i < 3; ++i) {
                List<String> result = Arrays.asList("true",
                    String.valueOf(16 + i),
                    String.valueOf((long) (16 + i)),
                    String.valueOf(12.2f + i),
                    String.valueOf((double) (12.2f + i)),
                    partitionFields.get(0),
                    partitionFields.get(1),
                    partitionFields.get(2));
                expectedResults.add(result);
            }
        }

        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
        );
        String[] rows = result.split("\n");
        assertEquals(9, rows.length);
        for (int i = 0; i < rows.length; ++i) {
            String[] parts = HiveTestUtils.parseOutput(rows[i]);
            int j = 0;
            for (String expectedValue : expectedResults.get(i)) {
                assertEquals(expectedValue, parts[j++]);
            }
        }
    }
}
