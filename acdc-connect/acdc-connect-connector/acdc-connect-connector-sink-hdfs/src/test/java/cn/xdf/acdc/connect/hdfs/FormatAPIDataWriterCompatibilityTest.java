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

import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroDataFileReader;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with DataWriter.
 */
public class FormatAPIDataWriterCompatibilityTest extends HiveTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new AvroDataFileReader();
        extension = ".avro";
    }

    @Test
    public void dataWriterNewFormatAPICompatibilityTest() {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        hdfsWriter.recover(TOPIC_PARTITION);

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0; offset < 7; offset++) {
            SinkRecord sinkRecord =
                new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
            sinkRecords.add(sinkRecord);
        }

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, "AUTO_CREATE_EXTERNAL_TABLE");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FULL");
        HdfsSinkConfig config = new HdfsSinkConfig(props);

        hdfsWriter = new HdfsWriterCoordinator(config, context);
        hdfsWriter.syncHiveMetaData(TOPIC_PARTITION);

        // Since we're not using a real format, we won't validate the output. However, this should at
        // least exercise the code paths for the old Format class

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    @Test
    public void dataWriterNewFormatAPICompatibilityWithDefaultsTest() {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        hdfsWriter.recover(TOPIC_PARTITION);

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0; offset < 7; offset++) {
            SinkRecord sinkRecord =
                new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
            sinkRecords.add(sinkRecord);
        }

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        Map<String, String> props = createProps();
        // Removing the entries below should test proper use of defaults in the connector's config.
        props.remove(StorageCommonConfig.STORAGE_CLASS_CONFIG);
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, "AUTO_CREATE_EXTERNAL_TABLE");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FULL");
        HdfsSinkConfig config = new HdfsSinkConfig(props);

        hdfsWriter = new HdfsWriterCoordinator(config, context);
        hdfsWriter.syncHiveMetaData(TOPIC_PARTITION);

        // Since we're not using a real format, we won't validate the output. However, this should at
        // least exercise the code paths for the old Format class

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
//    props.put(HdfsSinkConfig.FORMAT_CLASS_CONFIG, OldFormat.class.getName());
        // Enable Hive integration to make sure we exercise the paths that get HiveUtils
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE, "AUTO_CREATE_EXTERNAL_TABLE");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FULL");
        return props;
    }

}
