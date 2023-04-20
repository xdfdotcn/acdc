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

import cn.xdf.acdc.connect.hdfs.format.ProjectedResult;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.SchemaReader;
import cn.xdf.acdc.connect.hdfs.format.TableSchemaAndDataStatus;
import cn.xdf.acdc.connect.hdfs.format.text.TextRecordAppendWriterProvider;
import cn.xdf.acdc.connect.hdfs.hive.HiveTextTestBase;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.HiveMetaStoreConfigFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HiveMetaReaderTest extends HiveTextTestBase {

    private SchemaReader schemaReader;

    private StoreConfig storeConfig;

    private HdfsFileOperator fileOperator;

    private HiveUtil hiveUtil;

    private RecordWriterProvider recordWriterProvider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    private void initComponent() throws IOException {
        this.storeConfig = new HiveMetaStoreConfigFactory(
                connectorConfig,
                hiveMetaStore
        ).createStoreConfig();
        this.fileOperator = new HdfsFileOperator(
                new HdfsStorage(connectorConfig, connectorConfig.url()),
                storeConfig,
                connectorConfig
        );
        this.schemaReader = new HiveMetaReader(
                connectorConfig,
                storeConfig,
                fileOperator,
                hiveMetaStore);
        this.hiveUtil = schemaReader.getHiveFactory().createHiveUtil(storeConfig, connectorConfig, hiveMetaStore);
        recordWriterProvider = new TextRecordAppendWriterProvider(fileOperator, storeConfig);
    }

    @Test(expected = HiveMetaStoreException.class)
    public void testProjectRecordShouldThrownExceptionWithNotExistTable() throws IOException {
        super.cleanHive();
        initComponent();
        SinkRecord sinkRecord = createSinkRecord();
        schemaReader.projectRecord(TOPIC_PARTITION, sinkRecord);
    }

    @Test
    public void testProjectRecordShouldNotChangeSchema() throws HiveException, IOException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();
        Schema tableSchema = createPromotableSchema();
        setSupportSchemaChange(schemaReader, true);
        final ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createPromotableSchemaSinkRecord());
        tableSchema.fields().forEach(field -> assertEquals(field.schema().type(), projectRecord.getCurrentSchema().field(field.name()).schema().type()));

        assertTrue(!projectRecord.isNeedChangeSchema());
    }

    @Test
    public void testProjectRecordShouldChangeSchemaWhenFieldAdded() throws HiveException, IOException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();
        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createIncreaseFieldSinkRecord());
        assertTrue(projectRecord.isNeedChangeSchema());
        this.hiveUtil.alterSchema(projectRecord.getCurrentSchema());
        Table table = hiveMetaStore.getTable(storeConfig.database(), storeConfig.table());
        List<FieldSchema> cols = table.getCols();
        assertEquals(projectRecord.getCurrentSchema().fields().size(), cols.size());
        assertEquals("string", cols.get(cols.size() - 1).getName());
    }

    @Test
    public void testProjectRecordShouldNotChangeSchemaWithUnSupportSchemaChangeWhenFieldAdded() throws HiveException, IOException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();
        SinkRecord sinkRecord = createIncreaseFieldSinkRecord();
        setSupportSchemaChange(schemaReader, false);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, sinkRecord);
        assertTrue(!projectRecord.isNeedChangeSchema());
        Table table = hiveMetaStore.getTable(storeConfig.database(), storeConfig.table());
        List<FieldSchema> cols = table.getCols();
        assertEquals(projectRecord.getCurrentSchema().fields().size(), cols.size());
        assertEquals(sinkRecord.valueSchema().fields().size() - 1, projectRecord.getCurrentSchema().fields().size());
    }

    @Test
    public void testProjectRecordShouldNotChangeSchemaWhenFieldDrop() throws HiveException, IOException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();

        Schema tableSchema = createPromotableSchema();

        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createDecreaseFieldSinkRecord());
        assertTrue(!projectRecord.isNeedChangeSchema());
        assertEquals(tableSchema.fields().size(), projectRecord.getCurrentSchema().fields().size());
        String commitFile = fileOperator.createRotationCommittedFileInTablePartitionPath(
                "dt=20210715",
                TOPIC_PARTITION,
                ".txt"
        );
        assertTrue(!schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION).isExistData());
        RecordWriter recordWriter = recordWriterProvider.newRecordWriter(commitFile);
        recordWriter.write(projectRecord.getProjectedRecord());
        recordWriter.commit();
        assertTrue(schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION).isExistData());
        TableSchemaAndDataStatus tableSchemaAndDataStatus = schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION);
        for (String partition : tableSchemaAndDataStatus.getDataPartitions()) {
            hiveUtil.addPartition(partition);
        }
        assertEquals(hiveUtil.listPartitions().size(), tableSchemaAndDataStatus.getDataPartitions().size());
    }

    @Test
    public void testProjectRecordShouldPromotable() throws Exception {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();

        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createPromotableSinkRecord());
        assertTrue(!projectRecord.isNeedChangeSchema());
        String commitFile = fileOperator.createRotationCommittedFileInTablePartitionPath(
                "dt=20210715",
                TOPIC_PARTITION,
                ".txt"
        );
        assertTrue(!schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION).isExistData());
        RecordWriter recordWriter = recordWriterProvider.newRecordWriter(commitFile);
        recordWriter.write(projectRecord.getProjectedRecord());
        recordWriter.commit();
        assertTrue(schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION).isExistData());
        TableSchemaAndDataStatus tableSchemaAndDataStatus = schemaReader.getTableSchemaAndDataStatus(TOPIC_PARTITION);
        for (String partition : tableSchemaAndDataStatus.getDataPartitions()) {
            hiveUtil.addPartition(partition);
        }
    }

    @Test(expected = ConnectException.class)
    public void testProjectRecordShouldThrownExceptionWithNotPromotable() throws IOException, HiveException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();

        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createNotPromotableRecord());
        assertTrue(!projectRecord.isNeedChangeSchema());
    }

    @Test(expected = ConnectException.class)
    public void testProjectRecordShouldNotCompatibilityWhenNotSameFieldType() throws IOException, HiveException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();
        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createTypeChangeWithNotCompatibilityRecord());
        assertTrue(!projectRecord.isNeedChangeSchema());
    }

    @Test
    public void testProjectRecordShouldCompatibilityWhenHiveTableSchemaIsString() throws IOException, HiveException {
        super.cleanHive();
        super.createDefaultTable();
        initComponent();
        setSupportSchemaChange(schemaReader, true);
        ProjectedResult projectRecord = schemaReader.projectRecord(TOPIC_PARTITION, createTypeChangeRecordWithStringCompatibility());
        assertTrue(!projectRecord.isNeedChangeSchema());
    }

    private void setSupportSchemaChange(final SchemaReader schemaReader, boolean isSupportSchemaChange) {
        try {
            Class<?> clazz = schemaReader.getClass();
            Field field = clazz.getDeclaredField("isSupportSchemaChange");
            field.setAccessible(true);
            field.set(schemaReader, isSupportSchemaChange);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException();
        }
    }
}
