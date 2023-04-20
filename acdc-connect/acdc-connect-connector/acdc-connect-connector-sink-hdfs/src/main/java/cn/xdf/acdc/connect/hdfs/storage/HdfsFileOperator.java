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

package cn.xdf.acdc.connect.hdfs.storage;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.filter.CommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.filter.TableCommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.filter.TableTopicPartitionCommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;

@Slf4j
public class HdfsFileOperator {

    private final HdfsStorage hdfsStorage;

    private final HdfsSinkConfig hdfsSinkConfig;

    private final StoreConfig storeConfig;

    private final String zeroPadFormat;

    public HdfsFileOperator(
            final HdfsStorage hdfsStorage,
            final StoreConfig storeConfig,
            final HdfsSinkConfig hdfsSinkConfig
    ) {
        this.hdfsStorage = hdfsStorage;
        this.storeConfig = storeConfig;
        this.hdfsSinkConfig = hdfsSinkConfig;
        zeroPadFormat = "%0"
                + this.hdfsSinkConfig.getInt(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
                + "d";
    }

    /**
     * Get the hdfs storage.
     *
     * @return HdfsStorage
     */
    public HdfsStorage storage() {
        return hdfsStorage;
    }

    /**
     * Generate name of committed file.
     *
     * @param topicPartition kafka topic partition
     * @param startVersion   start version
     * @param endVersion     end version
     * @param extension      file extension
     * @return name of committed file
     */
    public String generateCommittedFileName(
            final TopicPartition topicPartition,
            long startVersion,
            long endVersion,
            final String extension
    ) {
        int partition = topicPartition.partition();
        StringBuilder sb = new StringBuilder();
        sb.append(topicPartition.topic());
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(partition);
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, startVersion));
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, endVersion));
        sb.append(extension);
        String commitFileName = sb.toString();
        return commitFileName;
    }

    /**
     * Create hive table partition path in hdfs.
     *
     * @param encodePartition the partition
     * @return full path of the table partition path in hdfs
     */
    public String createTablePartitionPath(final String encodePartition) {
        return FilePath.of(storeConfig.tablePath())
                .join(encodePartition)
                .build().path();
    }

    /**
     * Generate name of temp file.
     *
     * @param extension file  extension
     * @return name of temp file
     */
    public String generateTempFileName(final String extension) {
        UUID id = UUID.randomUUID();
        String name = id + "_" + "temp" + extension;
        return name;
    }

    /**
     * Create committed file in hive table partition path.
     *
     * @param encodePartition the partition
     * @param topicPartition  kafka topic partition
     * @param startVersion    startVersion
     * @param endVersion      endVersion
     * @param extension       file extension
     * @return full path of the committed file
     */
    public String createCommittedFileInTablePartitionPath(
            final String encodePartition,
            final TopicPartition topicPartition,
            long startVersion,
            long endVersion,
            final String extension
    ) {
        return FilePath.of(storeConfig.tablePath())
                .join(encodePartition)
                .join(generateCommittedFileName(
                        topicPartition,
                        startVersion,
                        endVersion,
                        extension)
                )
                .build().path();
    }

    /**
     * Create rotation committed file in hive table partition path.
     *
     * @param encodePartition the partition
     * @param topicPartition  kafka topic partition
     * @param extension       file extension
     * @return full path of the committed file
     */
    public String createRotationCommittedFileInTablePartitionPath(
            final String encodePartition,
            final TopicPartition topicPartition,
            final String extension

    ) {
        Optional<FileStatus> fileStatus = findCommittedFileWithMaxVersionForTopicPartitionInTablePartitionPath(topicPartition, encodePartition);
        long startVersion = NumberUtils.LONG_ZERO;
        long endVersion = fileStatus.isPresent()
                ? extractVersion(fileStatus.get().getPath().getName())
                : NumberUtils.LONG_ZERO;

        return FilePath.of(storeConfig.tablePath())
                .join(encodePartition)
                .join(generateCommittedFileName(
                        topicPartition,
                        startVersion,
                        endVersion + 1,
                        extension)
                )
                .build().path();
    }

    /**
     * Create temp file in temp table partition path.
     *
     * @param encodePartition the partition
     * @param extension       file extension
     * @return full path of the temp file
     */
    public String createTempFileInTempTablePartitionPath(final String encodePartition, final String extension) {
        return FilePath.of(storeConfig.tempTablePath())
                .join(encodePartition)
                .join(generateTempFileName(extension))
                .build().path();
    }

    /**
     * Get committed file with max version for topic partition in hive table path.
     *
     * @param topicPartition the kafka topic partition
     * @return the committed file with max version
     */
    public Optional<FileStatus> findCommittedFileWithMaxVersionForTopicPartitionInTablePath(final TopicPartition topicPartition) {
        CommittedFileFilter filter = new TableTopicPartitionCommittedFileFilter(topicPartition);

        Path path = new Path(
                FilePath.of(storeConfig.tablePath())
                        .build().path()
        );

        FileStatus fileStatus = findCommittedFileWithMaxVersionInPath(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Get committed file with max version for topic partition in hive table partition path.
     *
     * @param topicPartition  the kafka topic partition
     * @param encodePartition encodePartition
     * @return the committed file with max version
     */
    public Optional<FileStatus> findCommittedFileWithMaxVersionForTopicPartitionInTablePartitionPath(
            final TopicPartition topicPartition,
            final String encodePartition
    ) {
        CommittedFileFilter filter = new TableTopicPartitionCommittedFileFilter(topicPartition);

        Path path = new Path(
                FilePath.of(storeConfig.tablePath())
                        .join(encodePartition)
                        .build().path()
        );
        FileStatus fileStatus = findCommittedFileWithMaxVersionInPath(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Get committed file with max version in hive table path.
     *
     * @param topicPartition topic partition
     * @return the committed file with max version
     */
    public Optional<FileStatus> findCommittedFileWithMaxVersionInTablePath(final TopicPartition topicPartition) {
        TableCommittedFileFilter filter = new TableCommittedFileFilter(topicPartition);
        Path path = new Path(storeConfig.tablePath());
        FileStatus fileStatus = findCommittedFileWithMaxVersionInPath(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Obtain the version of the last record that was written to the specified HDFS file.
     *
     * <p>version: rotation number or kafka message offset
     *
     * <p>rotation number: used by {@link cn.xdf.acdc.connect.hdfs.writer.AtLeastOnceTopicPartitionWriter}
     *
     * <p>kafka message offset: used by {@link cn.xdf.acdc.connect.hdfs.writer.ExactlyOnceTopicPartitionWriter}
     *
     * @param filename the name of the HDFS file; may not be null
     * @return the offset of the last record written to the specified file in HDFS
     * @throws IllegalArgumentException if the filename does not match the expected pattern
     */
    public static long extractVersion(final String filename) {
        Matcher m = HdfsSinkConstants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        // NB: if statement has side effect of enabling group() call
        if (!m.matches()) {
            throw new IllegalArgumentException(filename + " does not match COMMITTED_FILENAME_PATTERN");
        }
        return Long.parseLong(m.group(HdfsSinkConstants.PATTERN_END_OFFSET_GROUP));
    }

    // CHECKSTYLE:OFF
    private FileStatus findCommittedFileWithMaxVersionInPath(
            final Path path,
            final CommittedFileFilter filter
    ) {
        if (!this.hdfsStorage.exists(path.toString())) {
            return null;
        }
        long maxVersion = -1L;
        FileStatus maxVersionFile = null;
        List<FileStatus> statuses = this.hdfsStorage.list(path.toString());
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                FileStatus fileStatus = findCommittedFileWithMaxVersionInPath(status.getPath(), filter);
                if (fileStatus != null) {
                    long version = extractVersion(fileStatus.getPath().getName());
                    if (version > maxVersion) {
                        maxVersion = version;
                        maxVersionFile = fileStatus;
                    }
                }
            } else {
                String filename = status.getPath().getName();
                log.trace("Checked for max offset: {}", status.getPath());
                if (filter.accept(status.getPath())) {
                    long version = extractVersion(filename);
                    if (version > maxVersion) {
                        maxVersion = version;
                        maxVersionFile = status;
                    }
                }
            }
        }
        return maxVersionFile;
    }
    // CHECKSTYLE:ON

    private ArrayList<FileStatus> getDirectories(final Path path) {
        List<FileStatus> statuses = this.hdfsStorage.list(path.toString());
        ArrayList<FileStatus> result = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                int count = 0;
                List<FileStatus> fileStatuses = this.hdfsStorage.list(status.getPath().toString());
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isDirectory()) {
                        result.addAll(getDirectories(fileStatus.getPath()));
                    } else {
                        count++;
                    }
                }
                if (count == fileStatuses.size()) {
                    result.add(status);
                }
            }
        }
        return result;
    }

    /**
     * Get hive table all partition file dir.
     *
     * @return all dirs of hdfs,in table path
     */
    public Optional<FileStatus[]> getTableDataPartitions() {
        if (!hdfsStorage.exists(storeConfig.tablePath())) {
            return Optional.ofNullable(null);
        }
        Path path = new Path(storeConfig.tablePath());
        ArrayList<FileStatus> result = getDirectories(path);
        return Optional.ofNullable(result.toArray(new FileStatus[result.size()]));
    }
}
