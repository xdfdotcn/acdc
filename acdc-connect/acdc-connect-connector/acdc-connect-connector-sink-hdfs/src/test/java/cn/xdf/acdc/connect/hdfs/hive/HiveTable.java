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

package cn.xdf.acdc.connect.hdfs.hive;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.kafka.connect.data.Schema;

public class HiveTable {

    /**
     * Create hive table by text format.
     * @param url  HDFS url
     * @param schema schema
     * @param partitioner partitioner
     * @param database database
     * @param tableName tableName
     * @param txtSeparator  txtSeparator
     * @return Table
     * @throws HiveException exception on create table
     */
    public Table createTxtTable(
        final String url,
        final Schema schema,
        final Partitioner partitioner,
        final String database,
        final String tableName,
        final String txtSeparator

    ) throws HiveException {
        String hdfsRoot = StoreConstants.HDFS_ROOT;
        String tablePath = FileUtils.jointPath(url, hdfsRoot, database, tableName);
        Table table = new Table(database, tableName);
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(LazySimpleSerDe.class.getName());
        table.setOwner("anonymous");
//        table.setDataLocation(new Path("hdfs://iZ2zegq48vciaafl9ywbaiZ:8020/user/hive/warehouse/test_sink"));

        table.setInputFormatClass(Format.TEXT.getInputFormatClass());
        table.setOutputFormatClass(Format.TEXT.getOutFormatClass());
        List<FieldSchema> columns = SchemaConverter.convertSchema(schema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        if (!Strings.isNullOrEmpty(txtSeparator)) {
            table.getSd().getSerdeInfo().getParameters().put(StoreConfig.TXT_SEPARATOR, txtSeparator);
        }
        return table;
    }

    /**
     * Create hive table by text format.
     * @param url  HDFS url
     * @param partitioner partitioner
     * @param columns hive table columns
     * @param txtSeparator  txtSeparator
     * @return Table
     * @throws HiveException exception on create table
     */
    public Table createTable(
        final String url,
        final List<FieldSchema> columns,
        final Partitioner partitioner,
        final String txtSeparator) throws HiveException {
        Preconditions.checkNotNull(txtSeparator);
        String tablePath = FilePath.of(url)
            .join(StoreConstants.HDFS_ROOT)
            .join(HdfsSinkConstants.DATA_DIRECTORY)
            .join(StoreConstants.HIVE_DB)
            .join(StoreConstants.HIVE_TABLE).build().path();

        Table table = new Table(StoreConstants.HIVE_DB, StoreConstants.HIVE_TABLE);
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(LazySimpleSerDe.class.getName());
        table.setOwner("anonymous");
//        table.setDataLocation(new Path("hdfs://iZ2zegq48vciaafl9ywbaiZ:8020/user/hive/warehouse/test_sink"));
        table.setInputFormatClass(Format.TEXT.getInputFormatClass());
        table.setOutputFormatClass(Format.TEXT.getOutFormatClass());
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        table.getParameters().put(StoreConfig.TXT_SEPARATOR, txtSeparator);
        return table;
    }
}
