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
import cn.xdf.acdc.connect.hdfs.filter.TableTpCommittedFileFilter;
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
     * @return HdfsStorage
     */
    public HdfsStorage storage() {
        return hdfsStorage;
    }

    /**
     * Generate name of commit file.
     * @param tableName  table name
     * @param tp  kafka topic partition
     * @param startVersion start version
     * @param endVersion end version
     * @param extension  file extension
     * @return name of commit file
     */
    public String commitFileName(
        final String tableName,
        final TopicPartition tp,
        long startVersion,
        long endVersion,
        final String extension
    ) {
        int partition = tp.partition();
        StringBuilder sb = new StringBuilder();
        sb.append(tableName);
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
     * Create commit file's partition.
     * @param encodePartition the partition
     * @return full path of the commit file
     */
    public String createPartitionOfCommitFile(final String encodePartition) {
        return FilePath.of(storeConfig.tablePath())
            .join(encodePartition)
            .build().path();
    }

    /**
     * Generate name of temp file.
     * @param extension file  extension
     * @return name of temp file
     */
    public String tempFileName(final String extension) {
        UUID id = UUID.randomUUID();
        String name = id.toString() + "_" + "tmp" + extension;
        return name;
    }

    /**
     * Create commit file by partition and tp.
     * @param encodePartition the partition
     * @param tp  kafka topic partition
     * @param startVersion  startVersion
     * @param endVersion endVersion
     * @param extension  file extension
     * @return full path of the commit file
     */
    public String createCommitFileByPartitionAndTp(
        final String encodePartition,
        final TopicPartition tp,
        long startVersion,
        long endVersion,
        final String extension
    ) {
        return FilePath.of(storeConfig.tablePath())
            .join(encodePartition)
            .join(commitFileName(
                storeConfig.table(),
                tp,
                startVersion,
                endVersion,
                extension)
            )
            .build().path();
    }

    /**
     * Create commit file by rotation.
     * @param encodePartition the partition
     * @param tp  kafka topic partition
     * @param extension  file extension
     * @return full path of the commit file
     */
    public String createCommitFileByRotation(
        final String encodePartition,
        final TopicPartition tp,
        final String extension

    ) {
        Optional<FileStatus> fileStatus = findMaxVerFileByPartitionAndTp(tp, encodePartition);
        long startVersion = NumberUtils.LONG_ZERO;
        long endVersion = fileStatus.isPresent()
            ? extractVersion(fileStatus.get().getPath().getName())
            : NumberUtils.LONG_ZERO;

        return FilePath.of(storeConfig.tablePath())
            .join(encodePartition)
            .join(commitFileName(
                storeConfig.table(),
                tp,
                startVersion,
                endVersion + 1,
                extension)
            )
            .build().path();
    }

    /**
     * Create temp file by partition .
     * @param encodePartition the partition
     * @param extension  file extension
     * @return full path of the temp file
     */
    public String createTempFileByPartition(final String encodePartition, final String extension) {
        return FilePath.of(storeConfig.tmpTablePath())
            .join(encodePartition)
            .join(tempFileName(extension))
            .build().path();
    }

    /**
     * Get max version file by specified tp .
     * @param tp the kafka topic partition
     * @return the max version file
     * */
    public Optional<FileStatus> findMaxVerFileByTp(final TopicPartition tp) {
        CommittedFileFilter filter = new TableTpCommittedFileFilter(
            tp,
            storeConfig.table()
        );

        Path path = new Path(
            FilePath.of(storeConfig.tablePath())
                .build().path()
        );

        FileStatus fileStatus = findMaxVersionFile(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Get max version file by specified partition and tp .
     * @param tp the kafka topic partition
     * @param encodePartition encodePartition
     * @return the max version file
     */
    public Optional<FileStatus> findMaxVerFileByPartitionAndTp(
        final TopicPartition tp,
        final String encodePartition
    ) {
        CommittedFileFilter filter = new TableTpCommittedFileFilter(
            tp,
            storeConfig.table()
        );
        Path path = new Path(
            FilePath.of(storeConfig.tablePath())
                .join(encodePartition)
                .build().path()
        );
        FileStatus fileStatus = findMaxVersionFile(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Get max version file by specified table.
     * @return the max version file
     */
    public Optional<FileStatus> findTableMaxVerFile() {
        TableCommittedFileFilter filter = new TableCommittedFileFilter(storeConfig.table());
        Path path = new Path(storeConfig.tablePath());
        FileStatus fileStatus = findMaxVersionFile(path, filter);
        return Optional.ofNullable(fileStatus);
    }

    /**
     * Obtain the offset of the last record that was written to the specified HDFS file.
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
    private FileStatus findMaxVersionFile(
        final Path path,
        final CommittedFileFilter filter
    ) {
        if (!this.hdfsStorage.exists(path.toString())) {
            return null;
        }
        long maxVersion = -1L;
        FileStatus maxVerFile = null;
        List<FileStatus> statuses = this.hdfsStorage.list(path.toString());
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                FileStatus fileStatus = findMaxVersionFile(status.getPath(), filter);
                if (fileStatus != null) {
                    long version = extractVersion(fileStatus.getPath().getName());
                    if (version > maxVersion) {
                        maxVersion = version;
                        maxVerFile = fileStatus;
                    }
                }
            } else {
                String filename = status.getPath().getName();
                log.trace("Checked for max offset: {}", status.getPath());
                if (filter.accept(status.getPath())) {
                    long version = extractVersion(filename);
                    if (version > maxVersion) {
                        maxVersion = version;
                        maxVerFile = status;
                    }
                }
            }
        }
        return maxVerFile;
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
     * Get hive table all partition file dir .
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
