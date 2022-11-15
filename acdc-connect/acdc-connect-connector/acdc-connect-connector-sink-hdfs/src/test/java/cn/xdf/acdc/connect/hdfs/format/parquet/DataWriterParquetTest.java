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
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

public class DataWriterParquetTest extends TestWithMiniDFSCluster {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new ParquetDataFileReader();
        extension = ".parquet";
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
//    props.put(HdfsSinkConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
        props.put(HdfsSinkConfig.STORAGE_FORMAT, "PARQUET");
        return props;
    }

    @Test
    public void testWriteRecord() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }
}
