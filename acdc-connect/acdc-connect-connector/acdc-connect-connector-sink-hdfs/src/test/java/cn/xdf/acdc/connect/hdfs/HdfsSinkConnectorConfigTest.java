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
import cn.xdf.acdc.connect.hdfs.partitioner.DailyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.FieldPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.HourlyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HdfsSinkConnectorConfigTest extends TestWithMiniDFSCluster {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test(expected = ConfigException.class)
    public void testUrlConfigMustBeNonEmpty() {
        properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
        properties.remove(HdfsSinkConfig.HDFS_URL_CONFIG);
        connectorConfig = new HdfsSinkConfig(properties);
    }

    @Test
    public void testStorageCommonUrlPreferred() {
        connectorConfig = new HdfsSinkConfig(properties);
        assertEquals(url, connectorConfig.url());
    }

    @Test
    public void testHdfsUrlIsValid() {
        connectorConfig = new HdfsSinkConfig(properties);
        properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
        assertEquals(url, connectorConfig.url());
    }

    @Test
    public void testStorageClass() throws Exception {
        // No real test case yet
        connectorConfig = new HdfsSinkConfig(properties);
        assertEquals(
            HdfsStorage.class,
            connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
        );
    }

    @Test
    public void testUndefinedURL() throws Exception {
        properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
        connectorConfig = new HdfsSinkConfig(properties);
        assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
    }

    @Test
    public void testAvroCompressionSettings() {
        for (String codec : HdfsSinkConfig.AVRO_SUPPORTED_CODECS) {
            Map<String, String> props = new HashMap<>(this.properties);
            props.put(HdfsSinkConfig.AVRO_CODEC_CONFIG, codec);
            HdfsSinkConfig config = new HdfsSinkConfig(props);
            Assert.assertNotNull(config.getAvroCodec());
        }
    }

    @Test(expected = ConfigException.class)
    public void testUnsupportedAvroCompressionSettings() {
        // test for an unsupported codec.
        this.properties.put(HdfsSinkConfig.AVRO_CODEC_CONFIG, "abc");

        new HdfsSinkConfig(properties);
        Assert.assertTrue("Expected the constructor to throw an exception", false);
    }

    @Test
    public void testValidTimezoneWithScheduleIntervalAccepted() {
        properties.put(PartitionerConfig.TIMEZONE_CONFIG, "CET");
        properties.put(HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
        new HdfsSinkConfig(properties);
    }

    @Test(expected = ConfigException.class)
    public void testEmptyTimezoneThrowsExceptionOnScheduleInterval() {
        properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
        properties.put(HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
        new HdfsSinkConfig(properties);
    }

    @Test
    public void testEmptyTimezoneExceptionMessage() {
        properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
        properties.put(HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
        String expectedError = String.format(
            "%s configuration must be set when using %s",
            PartitionerConfig.TIMEZONE_CONFIG,
            HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG
        );
        try {
            new HdfsSinkConfig(properties);
        } catch (ConfigException e) {
            assertEquals(expectedError, e.getMessage());
        }
    }

    @Test
    public void testRecommendedValues() throws Exception {
        List<Object> expectedStorageClasses = Arrays.<Object>asList(HdfsStorage.class);
//    List<Object> expectedFormatClasses = Arrays.<Object>asList(
//        AvroFormat.class,
//        JsonFormat.class,
//        OrcFormat.class,
//        ParquetFormat.class,
//        StringFormat.class
//    );
        List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        );

        List<ConfigValue> values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            if (val.value() instanceof Class) {
                switch (val.name()) {
                    case StorageCommonConfig.STORAGE_CLASS_CONFIG:
                        assertEquals(expectedStorageClasses, val.recommendedValues());
                        break;
                    case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
                        assertEquals(expectedPartitionerClasses, val.recommendedValues());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Test
    public void testVisibilityForPartitionerClassDependentConfigs() {
        properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        List<ConfigValue> values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertFalse(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                    assertTrue(val.visible());
                    break;
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertFalse(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                    assertFalse(val.visible());
                    break;
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                    assertFalse(val.visible());
                    break;
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            TimeBasedPartitioner.class.getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;
            }
        }

        Partitioner<?> klass = new Partitioner<FieldSchema>() {
            @Override
            public void configure(final Map<String, Object> config) {
            }

            @Override
            public String encodePartition(final SinkRecord sinkRecord) {
                return null;
            }

            @Override
            public String generatePartitionedPath(final String topic, final String encodedPartition) {
                return null;
            }

            @Override
            public List<FieldSchema> partitionFields() {
                return null;
            }
        };

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            klass.getClass().getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void testVisibilityForDeprecatedPartitionerClassDependentConfigs() throws Exception {
        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            DefaultPartitioner.class.getName()
        );
        List<ConfigValue> values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertFalse(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            FieldPartitioner.class.getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                    assertTrue(val.visible());
                    break;
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertFalse(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            DailyPartitioner.class.getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                    assertFalse(val.visible());
                    break;
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;
            }
        }

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            HourlyPartitioner.class.getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                    assertFalse(val.visible());
                    break;
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;

            }
        }

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            TimeBasedPartitioner.class.getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;

            }
        }

        Partitioner klass = new Partitioner() {
            @Override
            public String encodePartition(final SinkRecord sinkRecord) {
                return null;
            }

            @Override
            public String generatePartitionedPath(final String tableName, final String encodedPartition) {
                return null;
            }

            @Override
            public List partitionFields() {
                return null;
            }

            @Override
            public void configure(final Map config) {
            }
        };

        properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            klass.getClass().getName()
        );
        values = HdfsSinkConfig.getConfig().validate(properties);
        for (ConfigValue val : values) {
            switch (val.name()) {
                case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
                case PartitionerConfig.PATH_FORMAT_CONFIG:
                case PartitionerConfig.LOCALE_CONFIG:
                case PartitionerConfig.TIMEZONE_CONFIG:
                    assertTrue(val.visible());
                    break;
                default:
                    break;

            }
        }
    }
}
