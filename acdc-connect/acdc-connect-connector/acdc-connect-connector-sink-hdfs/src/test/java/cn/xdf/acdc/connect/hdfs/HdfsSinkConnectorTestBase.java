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

import cn.xdf.acdc.connect.core.sink.processor.CachedSinkProcessorProvider;
import cn.xdf.acdc.connect.core.sink.processor.ProcessorProvider;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import io.confluent.connect.avro.AvroData;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HdfsSinkConnectorTestBase extends StorageSinkTestBase {

    // CHECKSTYLE:OFF

    protected HdfsSinkConfig connectorConfig;

    protected ProcessorProvider processorProvider;

    protected Map<String, Object> parsedConfig;

    protected Configuration conf;

    protected AvroData avroData;

    protected static final String TOPIC_WITH_DOTS = "topic.with.dots";

    protected static final TopicPartition TOPIC_WITH_DOTS_PARTITION = new TopicPartition(TOPIC_WITH_DOTS, PARTITION);

    protected StoreContext defaultStoreContext;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
//        props.put(HdfsSinkConfig.HDFS_URL_CONFIG, url);
//        props.put(StorageCommonConfig.STORE_URL_CONFIG, url);
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "3");
        props.put(
                StorageCommonConfig.STORAGE_CLASS_CONFIG,
                "cn.xdf.acdc.connect.hdfs.storage.HdfsStorage"
        );
        props.put(
                PartitionerConfig.PARTITIONER_CLASS_CONFIG,
                DefaultPartitioner.class.getName()
        );
        props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
        props.put(
                PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
                String.valueOf(TimeUnit.HOURS.toMillis(1))
        );
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/");
        props.put(PartitionerConfig.LOCALE_CONFIG, "en");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, HiveIntegrationMode.NONE.name());

        // The default based on default configuration of 10
        props.put(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "10");

        return props;
    }

    protected Struct createRecord(final Schema schema, int ibase, float fbase) {
        return new Struct(schema)
                .put("boolean", true)
                .put("int", ibase)
                .put("long", (long) ibase)
                .put("float", fbase)
                .put("double", (double) fbase);
    }

    // Create a batch of records with incremental numeric field values. Total number of records is
    // given by 'size'.
    protected List<Struct> createRecordBatch(final Schema schema, int size) {
        ArrayList<Struct> records = new ArrayList<>(size);
        int ibase = 16;
        float fbase = 12.2f;

        for (int i = 0; i < size; ++i) {
            records.add(createRecord(schema, ibase + i, fbase + i));
        }
        return records;
    }

    // Create a list of records by repeating the same record batch. Total number of records: 'batchesNum' x 'batchSize'
    protected List<Struct> createRecordBatches(final Schema schema, int batchSize, int batchesNum) {
        ArrayList<Struct> records = new ArrayList<>();
        for (int i = 0; i < batchesNum; ++i) {
            records.addAll(createRecordBatch(schema, batchSize));
        }
        return records;
    }

    //@Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        connectorConfig = new HdfsSinkConfig(properties);
        parsedConfig = new HashMap<>(connectorConfig.plainValues());
        conf = connectorConfig.getHadoopConfiguration();
        avroData = new AvroData(connectorConfig.avroDataConfig());
        processorProvider = new CachedSinkProcessorProvider(connectorConfig);
        defaultStoreContext = createStoreContext();
    }

    private StoreContext createStoreContext() throws IOException {
        return StoreContext.buildContext(connectorConfig);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
