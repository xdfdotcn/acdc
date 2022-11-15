/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.format.orc;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestUtils;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.DailyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.FieldPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeUtils;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.writer.HdfsWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class HiveIntegrationOrcTest extends HiveTestBase {

    private final Map<String, String> localProps = new HashMap<>();

    public HiveIntegrationOrcTest() {
    }

    @Before
    public void beforeTest() {
//    localProps.put(HdfsSinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, hiveTableNameConfig);
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConfig.SHUTDOWN_TIMEOUT_CONFIG, "10000");
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.ORC.name());
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, "AUTO_CREATE_EXTERNAL_TABLE");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FULL");
        props.putAll(localProps);
        return props;
    }

    /**
     * should be omitted in order to be able to add properties per test.
     * @throws Exception exception on set up
     */

    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testSyncWithHiveOrc() throws Exception {
        setUp();
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();
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
    public void testHiveIntegrationOrc() throws Exception {
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

        List<String> expectedPartitions = Arrays.asList(partitionLocation(PARTITION));

        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short) -1);

        assertEquals(expectedPartitions, partitions);
    }

    /**
     * 分区字段使用表schema中的字段，则表中的schema会缺省此字段的创建.
     */
    @Test
    public void testHiveIntegrationFieldPartccionerOrc() throws Exception {
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
        localProps.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
        setUp();
        HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig, context);
        Schema schema = createSchema();

        List<Struct> records = createRecordBatches(schema, 3, 3);
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);
        Schema processSchema = processorProvider.getProcessor(StoreConstants.TABLES)
            .process(sinkRecords.get(0)).valueSchema();

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : processSchema.fields()) {
            expectedColumnNames.add(field.name());
        }
        //分区字段使用表schema中的字段，则表中的schema会缺省此字段的创建
//        expectedColumnNames.remove("int");
//        List<String> actualColumnNames = new ArrayList<>();
        assertTrue(expectedColumnNames.size() == table.getSd().getCols().size());
        for (FieldSchema column : table.getSd().getCols()) {
            expectedColumnNames.contains(column.getName());
        }
//        assertEquals(expectedColumnNames, actualColumnNames);

        List<String> partitionFieldNames = connectorConfig.getList(
            PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
        );
        String partitionFieldName = partitionFieldNames.get(0);
        String directory1 = partitionFieldName + "=" + 16;
        String directory2 = partitionFieldName + "=" + 17;
        String directory3 = partitionFieldName + "=" + 18;

        List<String> expectedPartitions = new ArrayList<>();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
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

        List<List<String>> expectedResults = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                List<String> result = new ArrayList<>();
                for (Field field : processSchema.fields()) {
                    result.add(String.valueOf(records.get(i).get(field.name())));
                }
                expectedResults.add(result);
            }
        }

        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + storeConfig.table()
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

    @Test
    public void testHiveIntegrationTimeBasedPartitionerOrc() throws Exception {
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
        setUp();
        HdfsWriter hdfsWriter = new HdfsWriter(connectorConfig, context);

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);
        Schema processSchema = processorProvider.getProcessor(StoreConstants.TABLES)
            .process(sinkRecords.get(0)).valueSchema();

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        String hiveTableName = defaultStoreContext.getStoreConfig().table();
        String hiveDatabase = defaultStoreContext.getStoreConfig().database();
        Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field : processSchema.fields()) {
            expectedColumnNames.add(field.name());
        }

        assertTrue(expectedColumnNames.size() == table.getSd().getCols().size());
        for (FieldSchema column : table.getSd().getCols()) {
            expectedColumnNames.contains(column.getName());
        }
//        assertEquals(expectedColumnNames, actualColumnNames);

        String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd";
        DateTime dateTime = DateTime.now(DateTimeZone.forID("America/Los_Angeles"));
        String encodedPartition = TimeUtils
            .encodeTimestamp(TimeUnit.HOURS.toMillis(24), pathFormat, "America/Los_Angeles",
                dateTime.getMillis());
        String directory = encodedPartition;
        List<String> expectedPartitions = new ArrayList<>();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();

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

//        List<List<String>> expectedResults = new ArrayList<>();
//        for (int j = 0; j < 3; ++j) {
//            for (int i = 0; i < 3; ++i) {
//                List<String> result = Arrays.asList("true",
//                    String.valueOf(16 + i),
//                    String.valueOf((long) (16 + i)),
//                    String.valueOf(12.2f + i),
//                    String.valueOf((double) (12.2f + i)),
//                    partitionFields.get(0),
//                    partitionFields.get(1),
//                    partitionFields.get(2));
//                expectedResults.add(result);
//            }
//        }

        List<List<String>> expectedResults = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                List<String> result = new ArrayList<>();
                for (Field field : processSchema.fields()) {
                    result.add(String.valueOf(records.get(i).get(field.name())));
                }
                result.add(partitionFields.get(0));
                result.add(partitionFields.get(1));
                result.add(partitionFields.get(2));

                expectedResults.add(result);

            }
        }

        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
        );
        String[] rows = result.split("\n");
        List<String> rowList = Arrays.stream(rows).sorted(Comparator.comparing(s -> HiveTestUtils.parseOutput(s)[1])).collect(Collectors.toList());
        assertEquals(9, rows.length);
        for (int i = 0; i < rows.length; ++i) {
            String[] parts = HiveTestUtils.parseOutput(rowList.get(i));
            int j = 0;
            for (String expectedValue : expectedResults.get(i)) {
                assertEquals(expectedValue, parts[j++]);
            }
        }
    }

}
