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

import cn.xdf.acdc.connect.hdfs.DataFileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;

public class OrcDataFileReader implements DataFileReader {

    @Override
    public Collection<Object> readData(final Configuration conf, final Path path) throws IOException {
        ArrayList<Object> collection = new ArrayList<>();
        OrcFile.ReaderOptions readerOptions = new OrcFile.ReaderOptions(conf);
        Reader reader = OrcFile.createReader(path, readerOptions);

        RecordReader rows = reader.rows();

        Object row = null;
        while (rows.hasNext()) {
            row = rows.next(row);
            collection.add(row);
        }

        rows.close();

        return collection;
    }
}
