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

import cn.xdf.acdc.connect.hdfs.format.avro.AvroDataFileReader;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with DataWriter.
 */
public class CustomPartitionerPropertiesTest extends HiveTestBase {

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        props.put(CustomPartitioner.CUSTOM_PROPERTY, CustomPartitioner.EXPECTED_VALUE);
        return props;
    }

    /**
     * should be omitted in order to be able to add properties per test.
     * @throws Exception exception on set up
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new AvroDataFileReader();
        extension = ".avro";
    }

    @Test
    public void createDataWriterWithCustomPartitioner() {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        Partitioner<?> partitioner = hdfsWriter.getPartitioner();
        assertEquals(CustomPartitioner.class.getName(), partitioner.getClass().getName());
        CustomPartitioner customPartitioner = (CustomPartitioner) partitioner;
        assertEquals(CustomPartitioner.EXPECTED_VALUE, customPartitioner.customValue());

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    public static final class CustomPartitioner extends DefaultPartitioner {

        public static final String CUSTOM_PROPERTY = "custom.property";

        public static final String EXPECTED_VALUE = "expectThis";

        private String customValue;

        @Override
        public void configure(final Map config) {
            super.configure(config);
            this.customValue = (String) config.get(CUSTOM_PROPERTY);
        }

        /**
         * Custom value.
         * @return custom value
         */
        public String customValue() {
            return this.customValue;
        }
    }

}
