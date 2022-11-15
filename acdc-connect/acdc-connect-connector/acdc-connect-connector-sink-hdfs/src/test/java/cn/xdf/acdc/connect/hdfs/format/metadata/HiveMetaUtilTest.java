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

package cn.xdf.acdc.connect.hdfs.format.metadata;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.StorageSinkTestBase;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 {@link HiveMetaUtil}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SinkSchemas.class)
@PowerMockIgnore("javax.management.*")
public class HiveMetaUtilTest extends StorageSinkTestBase {

    @Mock
    private HiveMetaStore hiveMetaStore;

    @Mock
    private StoreConfig storeConfig;

    @Mock
    private HdfsSinkConfig hdfsSinkConfig;

    @Mock
    private Partitioner partitioner;

    @Mock
    private Schema schema;

    private HiveUtil hiveUtil;

    @Before
    public void setUp() throws Exception {
        hiveUtil = new HiveMetaUtil(storeConfig, hdfsSinkConfig, hiveMetaStore);
    }

    @Test
    public void testCreateTableShouldInvokeWithNothing() {
        hiveUtil.createTable(schema, partitioner);
    }

    @Test
    public void testAlertTableShouldMethInvokeWithAlterSchemaMethod() {
        Table table = new Table(StoreConstants.HIVE_DB, StoreConstants.HIVE_TABLE);
        List<FieldSchema> fieldSchemaList = createFieldSchemaList();
        Capture<Table> captureTable = EasyMock.newCapture();

        EasyMock.expect(storeConfig.database()).andReturn(StoreConstants.HIVE_DB);
//        EasyMock.expect(schema.type()).andReturn(Type.STRUCT);
        EasyMock.expect(storeConfig.table()).andReturn(StoreConstants.HIVE_TABLE);
        EasyMock.expect(hiveMetaStore.getTable(EasyMock.anyString(), EasyMock.anyString())).andReturn(table);
        hiveMetaStore.alterTable(EasyMock.capture(captureTable));
        // static class mock necessary @PrepareForTest(ConnectToHiveSchemaConverter.class)
        PowerMock.mockStatic(SinkSchemas.class);
        EasyMock.expect(SinkSchemas.convertToFieldColumns(EasyMock.anyObject())).andReturn(fieldSchemaList);
        PowerMock.replayAll(storeConfig, hiveMetaStore, schema, SinkSchemas.class);
        hiveUtil.alterSchema(schema);
        Assert.assertTrue(table == captureTable.getValue());
        PowerMock.verifyAll();
    }

    private List<FieldSchema> createFieldSchemaList() {
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(new FieldSchema());
        return fieldSchemaList;
    }
}
