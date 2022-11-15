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

package cn.xdf.acdc.connect.hdfs.format.orc;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.hive.SchemaConverter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class DataWriterOrcTest extends TestWithMiniDFSCluster {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new OrcDataFileReader();
        extension = ".orc";
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConfig.STORAGE_FORMAT, Format.ORC.toString());
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

    @Override
    protected void verifyContents(final List<SinkRecord> expectedRecords, int start, final Collection<Object> records) {
        int startIndex = start;
        Schema expectedSchema = null;
        for (Object orcRecord : records) {
            if (expectedSchema == null) {
                expectedSchema = expectedRecords.get(startIndex).valueSchema();
            }
            Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                expectedRecords.get(startIndex++).value(),
                expectedSchema);

            TypeInfo typeInfo = SchemaConverter.convert(expectedSchema);

            ArrayList<Object> objs = new ArrayList<>();
            for (Field field : expectedSchema.fields()) {
                objs.add(((Struct) expectedValue).get(field));
            }

            expectedValue = OrcUtil.createOrcStruct(typeInfo, objs.toArray(new Object[0]));

            assertEquals(expectedValue.toString(), orcRecord.toString());
        }
    }

}
