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

package cn.xdf.acdc.connect.hdfs.partitioner;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;

public class PartitionerFactory {

    /**
     * Create a partitioner by config .
     * @param config  config
     * @return the partitioner
     */
    public static Partitioner createPartitioner(final HdfsSinkConfig config) {
        try {
            Class<? extends Partitioner> partitionerClass = (Class<? extends Partitioner>)
                config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
            Partitioner partitioner = partitionerClass.newInstance();
            partitioner.configure(new HashMap<>(config.plainValues()));
            return partitioner;
        } catch (ReflectiveOperationException e) {
            throw new ConnectException(e);
        }
    }
}
