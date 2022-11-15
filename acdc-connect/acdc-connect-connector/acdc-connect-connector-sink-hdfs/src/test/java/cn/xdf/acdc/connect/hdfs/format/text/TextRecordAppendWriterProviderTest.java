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

package cn.xdf.acdc.connect.hdfs.format.text;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.common.Schemas;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.metadata.HiveMetaUtil;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveTable;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestUtils;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.writer.HiveMetaRestorer;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 {@link TextRecordAppendWriterProvider}.
 */
public class TextRecordAppendWriterProviderTest extends HiveTestBase {

    private static final String DATE_FORMATTER = "yyyy-MM-dd";

    private static final String TIME_FORMATTER = "HH:mm:ss";

    private static final String TIMESTAMP_FORMATTER = "yyyy-MM-dd HH:mm:ss";

    private Partitioner partitioner;

    private HiveMetaStore hiveMetaStore;

    private HdfsFileOperator hdfsFileOperator;

    private HiveMetaRestorer hiveMetaRestorer;

    private StoreConfig storeConfig;

    private HiveUtil hiveUtil;

    private SimpleDateFormat dateFormat;

    private SimpleDateFormat timeFormat;

    private SimpleDateFormat timestampFormat;

    private java.util.Date now;

    private java.util.Date date;

    private java.util.Date time;

    private java.util.Date timestamp;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        props.put(HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR, ",");
        return props;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        hiveMetaStore = new HiveMetaStore(connectorConfig);
        StoreContext storeContext = StoreContext.buildContext(connectorConfig, hiveMetaStore);
        partitioner = storeContext.getPartitioner();
        hdfsFileOperator = storeContext.getFileOperator();
        storeConfig = storeContext.getStoreConfig();
        hiveMetaRestorer = storeContext.getHiveMetaRestorer();
        hiveUtil = new HiveMetaUtil(storeConfig, connectorConfig, hiveMetaStore);

        dateFormat = new SimpleDateFormat(DATE_FORMATTER);
        timeFormat = new SimpleDateFormat(TIME_FORMATTER);
        timestampFormat = new SimpleDateFormat(TIMESTAMP_FORMATTER);

        now = new java.util.Date();
        date = dateFormat.parse(dateFormat.format(now));
        time = timeFormat.parse(timeFormat.format(now));
        timestamp = timestampFormat.parse(timestampFormat.format(now));
    }

    @Test
    public void testWriteShouldSuccess() throws Exception {
        CountDownLatch cdLatch = new CountDownLatch(1);
        List<FieldSchema> fieldSchemaList = Schemas.createHivePrimitiveSchemaWithAllFieldType();
        SinkRecord sinkRecord = Schemas.createHivePrimitiveRecordWithAllFieldType(date, timestamp);
        Table table = new HiveTable().createTable(url, fieldSchemaList, partitioner, storeConfig.textSeparator());
        hiveMetaStore.createTable(table);
        // write
        TextRecordAppendWriterProvider textRecordAppendWriterProvider = new TextRecordAppendWriterProvider(hdfsFileOperator, storeConfig);
        String commitFileName = hdfsFileOperator.createCommitFileByRotation(
            partitioner.encodePartition(null),
            TOPIC_PARTITION,
            textRecordAppendWriterProvider.getExtension());
        RecordWriter recordWriter = textRecordAppendWriterProvider.newRecordWriter(commitFileName);
        List<Object> data = TextUtil.convertStruct((Struct) sinkRecord.value());
        recordWriter.write(sinkRecord);
        hiveMetaRestorer.addPartitionIfAbsent(partitioner.encodePartition(null));
        recordWriter.close();
        new Thread(() -> {
            while (true) {
                if (hiveUtil.listPartitions().isEmpty()) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException();
                    }
                } else {
                    break;
                }
            }
            cdLatch.countDown();
        }).start();

        cdLatch.await();
        // hive select
        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(storeConfig.table())
        );
        String[] rows = result.split("\n");
        List<String> rowList = Arrays.stream(rows).sorted(Comparator.comparing(s -> HiveTestUtils.parseOutput(s)[1])).collect(Collectors.toList());
        assertEquals(1, rows.length);
        String[] parts = HiveTestUtils.parseOutput(rowList.get(0));

        // partition table, more "dt" field
        assertEquals(data.size(), parts.length - 1);
        for (int i = 0; i < rowList.size(); i++) {
            String valueStr = String.valueOf(data.get(i));
            // Hdfs 2021-08-10 03:32:51.0
            if (valueStr.contains("-")) {
                String dateStr = valueStr.substring(0, valueStr.lastIndexOf("."));
                dateStr = dateStr.replace(" ", "");
                assertEquals(dateStr, String.valueOf(parts[i]));
            } else {
                assertEquals(String.valueOf(data.get(i)), String.valueOf(parts[i]));
            }
        }
    }

    @Test
    public void testWriteShouldGetNullWhenSubStringTimestampFiled() throws Exception {
        CountDownLatch cdLatch = new CountDownLatch(1);
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(new FieldSchema("timestamp_schema", "timestamp", null));
        Table table = new HiveTable().createTable(url, fieldSchemaList, partitioner, storeConfig.textSeparator());
        hiveMetaStore.createTable(table);
        // Write
        TextRecordAppendWriterProvider textRecordAppendWriterProvider = new TextRecordAppendWriterProvider(hdfsFileOperator, storeConfig);
        String commitFileName = hdfsFileOperator.createCommitFileByRotation(
            partitioner.encodePartition(null),
            TOPIC_PARTITION,
            textRecordAppendWriterProvider.getExtension());
        hiveMetaRestorer.addPartitionIfAbsent(partitioner.encodePartition(null));
        // use append mode
        OutputStream out = hdfsFileOperator.storage().append(commitFileName);
        OutputStreamWriter streamWriter = new OutputStreamWriter(out, Charset.defaultCharset());
        BufferedWriter writer = new BufferedWriter(streamWriter, 1000);
        writer.write("04:31:31");
        writer.newLine();
        writer.close();
        new Thread(() -> {
            while (true) {
                if (hiveUtil.listPartitions().isEmpty()) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException();
                    }
                } else {
                    break;
                }
            }
            cdLatch.countDown();
        }).start();

        cdLatch.await();
        // hive select
        String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(storeConfig.table())
        );
        String[] rows = result.split("\n");
        List<String> rowList = Arrays.stream(rows).sorted(Comparator.comparing(s -> HiveTestUtils.parseOutput(s)[1])).collect(Collectors.toList());
        assertEquals(1, rows.length);
        String[] parts = HiveTestUtils.parseOutput(rowList.get(0));
        assertEquals("NULL", parts[0]);
    }
}
