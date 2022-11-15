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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.storage.schema.StorageSchemaCompatibility;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractSchemaFileReader implements SchemaReader {

    private final StoreConfig storeConfig;

    private final HdfsFileOperator fileOperator;

    private final HdfsSinkConfig hdfsSinkConfig;

    private final StorageSchemaCompatibility compatibility;

    private Schema curSchema;

    public AbstractSchemaFileReader(
        final HdfsSinkConfig hdfsSinkConfig,
        final StoreConfig storeConfig,
        final HdfsFileOperator fileOperator
    ) {
        this.hdfsSinkConfig = hdfsSinkConfig;
        this.storeConfig = storeConfig;
        this.fileOperator = fileOperator;
        this.compatibility = StorageSchemaCompatibility.getCompatibility(
            this.hdfsSinkConfig.getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG)
        );
    }

    /**
     * Get schema by file path .
     * @param path  file path
     * @return schema
     */
    public abstract Schema getSchema(Path path);

    @Override
    public ProjectedResult projectRecord(final TopicPartition tp, final SinkRecord sinkRecord) {
        boolean shouldChangeSchema = shouldChangeSchema(tp, sinkRecord);
        return ProjectedResult.builder()
            .projectedRecord(compatibility.project(sinkRecord, null, curSchema))
            .currentSchema(curSchema)
            .needChangeSchema(shouldChangeSchema)
            .build();
    }

    protected boolean shouldChangeSchema(final TopicPartition tp, final SinkRecord sinkRecord) {
        // If  compatibility not is NONE,read Hdfs file's schema
        if (null == curSchema && compatibility != StorageSchemaCompatibility.NONE) {
            Optional<FileStatus> fileStatus = fileOperator.findMaxVerFileByTp(tp);
            if (fileStatus.isPresent()) {
                curSchema = getSchema(fileStatus.get().getPath());
            }
        }
        // Hdfs not exist file , use record's schema
        if (null == curSchema) {
            curSchema = sinkRecord.valueSchema();
            return true;
        }

        boolean shouldChangeSchema = compatibility.shouldChangeSchema(sinkRecord, null, curSchema);
        if (shouldChangeSchema) {
            curSchema = sinkRecord.valueSchema();
        }
        return shouldChangeSchema;
    }

    @Override
    public TableSchemaAndDataStatus getTableSchemaAndDataStatus() {
        Optional<FileStatus> fileStatus = fileOperator.findTableMaxVerFile();
        if (!fileStatus.isPresent()) {
            return TableSchemaAndDataStatus.builder()
                .existData(false)
                .build();
        }
        FileStatus[] fileStatuses = fileOperator.getTableDataPartitions().orElseGet(() -> new FileStatus[] {});
        List<String> dataPartitions = new ArrayList<>();
        for (FileStatus status : fileStatuses) {
            String location = status.getPath().toString();
            String partitionValue = getPartitionValue(location);
            dataPartitions.add(partitionValue);
        }

        return TableSchemaAndDataStatus.builder()
            .existData(true)
            .schema(getSchema(fileStatus.get().getPath()))
            .dataPartitions(dataPartitions)
            .build();
    }

    private String getPartitionValue(final String path) {
        String[] parts = path.split("/");
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        for (int i = 3; i < parts.length; ++i) {
            sb.append(parts[i]);
            sb.append("/");
        }
        return sb.toString();
    }

    protected StoreConfig getStoreConfig() {
        return storeConfig;
    }

    protected HdfsFileOperator getFileOperator() {
        return fileOperator;
    }

    protected HdfsSinkConfig getHdfsSinkConfig() {
        return hdfsSinkConfig;
    }
}
