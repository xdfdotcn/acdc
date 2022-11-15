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

import cn.xdf.acdc.connect.hdfs.DataFileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetDataFileReader implements DataFileReader {

    @Override
    public Collection<Object> readData(final Configuration conf, final Path path) throws IOException {
        Collection<Object> result = new ArrayList<>();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
        ParquetReader<GenericRecord> parquetReader = builder.withConf(conf).build();
        GenericRecord record;
        while ((record = parquetReader.read()) != null) {
            result.add(record);
        }
        parquetReader.close();
        return result;
    }
}
