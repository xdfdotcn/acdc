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

package cn.xdf.acdc.connect.hdfs.format.avro;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.AbstractSchemaFileReader;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class AvroFileReader extends AbstractSchemaFileReader {

    private AvroData avroData;

    public AvroFileReader(final HdfsSinkConfig sinkConfig, final StoreConfig storeConfig, final HdfsFileOperator fileOperator
    ) {
        super(sinkConfig, storeConfig, fileOperator);
        this.avroData = new AvroData(fileOperator.storage().conf().avroDataConfig());
    }

    @Override
    public Schema getSchema(final Path path) {
        try {
            SeekableInput input = new FsInput(path, getHdfsSinkConfig().getHadoopConfiguration());
            DatumReader<Object> reader = new GenericDatumReader<>();
            FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
            org.apache.avro.Schema schema = fileReader.getSchema();
            fileReader.close();
            return avroData.toConnectSchema(schema);
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public HiveFactory getHiveFactory() {
        return new AvroHiveFactory(getFileOperator().storage());
    }
}
