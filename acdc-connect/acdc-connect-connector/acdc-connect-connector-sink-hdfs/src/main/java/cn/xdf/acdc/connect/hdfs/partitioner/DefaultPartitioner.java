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

import java.util.List;
import java.util.Map;

import cn.xdf.acdc.connect.hdfs.common.SchemaGenerator;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Default partitioner.
 * @param <T> The type representing the field schemas.
 */
public class DefaultPartitioner<T> implements Partitioner<T> {

    private static final String PARTITION_FIELD = "partition";

    private static final String SCHEMA_GENERATOR_CLASS =
        "cn.xdf.acdc.connect.hdfs.hive.schema.DefaultSchemaGenerator";

    private Map<String, Object> config;

    private List<T> partitionFields;

    private String delim;

    @Override
    public void configure(final Map<String, Object> config) {
        this.config = config;
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(final SinkRecord sinkRecord) {
        return PARTITION_FIELD + "=" + String.valueOf(sinkRecord.kafkaPartition());
    }

    @Override
    public String generatePartitionedPath(final String tableName, final String encodedPartition) {
        return tableName + delim + encodedPartition;
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(PARTITION_FIELD);
        }
        return partitionFields;
    }

    /**
     * Get schema generator by config.
     * @param config  config
     * @return the schema generator
     */
    public SchemaGenerator<T> newSchemaGenerator(final Map<String, Object> config) {
        Class<? extends SchemaGenerator<T>> generatorClass = null;
        try {
            generatorClass = getSchemaGeneratorClass();
            return generatorClass.newInstance();
        } catch (ClassNotFoundException
            | ClassCastException
            | IllegalAccessException
            | InstantiationException e) {
            ConfigException ce = new ConfigException("Invalid generator class: " + generatorClass);
            ce.initCause(e);
            throw ce;
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends SchemaGenerator<T>> getSchemaGeneratorClass()
        throws ClassNotFoundException {
        return (Class<? extends SchemaGenerator<T>>) Class.forName(SCHEMA_GENERATOR_CLASS);
    }

    protected Map<String, Object> getConfig() {
        return config;
    }

    List<T> getPartitionFields() {
        return partitionFields;
    }

    protected void setPartitionFields(final List<T> partitionFields) {
        this.partitionFields = partitionFields;
    }

    protected String getDelim() {
        return delim;
    }

    protected void setDelim(final String delim) {
        this.delim = delim;
    }
}
