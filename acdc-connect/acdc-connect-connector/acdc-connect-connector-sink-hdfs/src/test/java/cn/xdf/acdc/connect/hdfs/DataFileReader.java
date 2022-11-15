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

package cn.xdf.acdc.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface that corresponds to SchemaFileReader but reads data objects. Only used to validate
 * output during tests.
 */
public interface DataFileReader {

    /**
     * Read data.
     * @param conf  hadoop config
     * @param path  file path
     * @return data list
     * @throws IOException exception on read data
     */
    Collection<Object> readData(Configuration conf, Path path) throws IOException;
}
