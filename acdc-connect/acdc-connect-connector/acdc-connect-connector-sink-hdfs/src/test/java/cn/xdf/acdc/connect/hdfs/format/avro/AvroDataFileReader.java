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

import cn.xdf.acdc.connect.hdfs.DataFileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AvroDataFileReader implements DataFileReader {

    @Override
    public Collection<Object> readData(final Configuration conf, final Path path) throws IOException {
        ArrayList<Object> collection = new ArrayList<>();
        SeekableInput input = new FsInput(path, conf);
        DatumReader<Object> reader = new GenericDatumReader<>();
        FileReader<Object> fileReader = org.apache.avro.file.DataFileReader.openReader(input, reader);
        for (Object object : fileReader) {
            collection.add(object);
        }
        fileReader.close();
        return collection;
    }
}
