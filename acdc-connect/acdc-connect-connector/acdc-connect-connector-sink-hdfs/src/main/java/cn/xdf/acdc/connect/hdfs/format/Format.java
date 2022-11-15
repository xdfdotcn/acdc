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

package cn.xdf.acdc.connect.hdfs.format;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public enum Format {

    AVRO("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"),

    ORC("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),

    TEXT("org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),

    PARQUET("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

    private static final Map<String, Format> FORMAT_MAP = new HashedMap();

    static {
        for (Format type : Format.values()) {
            String input = type.getInputFormatClass();
            String out = type.getOutFormatClass();
            FORMAT_MAP.put(input, type);
            FORMAT_MAP.put(out, type);
        }
    }

    private String outFormatClass;

    private String inputFormatClass;

    Format(final String inputFormatClass, final String outFormatClass) {
        this.inputFormatClass = inputFormatClass;
        this.outFormatClass = outFormatClass;
    }

    /**
     * Get out format class .
     * @return out format class name
     */
    public String getOutFormatClass() {
        return outFormatClass;
    }

    /**
     * Get input format class .
     * @return out format class name
     */
    public String getInputFormatClass() {
        return inputFormatClass;
    }

    /**
     * Mapping by class name .
     * @param className  class name
     * @return Format
     */
    public static Format classNameOf(final String className) {
        Format format = FORMAT_MAP.get(className);
        if (null == format) {
            throw new ConfigException("UNKNOWN Type:" + className);
        }
        return FORMAT_MAP.get(className);
    }
}
