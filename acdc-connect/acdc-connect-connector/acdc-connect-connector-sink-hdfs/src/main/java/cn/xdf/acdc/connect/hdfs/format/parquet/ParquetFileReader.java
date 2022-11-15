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
import cn.xdf.acdc.connect.hdfs.format.AbstractSchemaFileReader;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetFileReader extends AbstractSchemaFileReader {

    private AvroData avroData;

    public ParquetFileReader(
        final HdfsSinkConfig sinkConfig,
        final StoreConfig storeConfig,
        final HdfsFileOperator fileOperator
    ) {
        super(sinkConfig, storeConfig, fileOperator);
        this.avroData = new AvroData(fileOperator.storage().conf().avroDataConfig());
    }

    @Override
    public Schema getSchema(final Path path) {
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
        try {
            ParquetReader<GenericRecord> parquetReader = builder.withConf(getHdfsSinkConfig().getHadoopConfiguration())
                .build();
            GenericRecord record;
            Schema schema = null;
            while ((record = parquetReader.read()) != null) {
                schema = avroData.toConnectSchema(record.getSchema());
            }
            parquetReader.close();
            return schema;
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public HiveFactory getHiveFactory() {
        return new ParquetHiveFactory();
    }
}
