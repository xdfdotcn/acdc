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

package cn.xdf.acdc.connect.hdfs.utils;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.filter.CommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.filter.TableTpCommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.FilePath.FilePathBuilder;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import com.google.common.base.Strings;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;


/**
 Instead of FilePath and  HdfsFileOperator.
 // CHECKSTYLE:OFF
 */

@Deprecated
public class FileUtils {

    private static final String PATH_SEPARATOR = "/";

    private static final String PATH_REPAIR_REG = "^/|/$";

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    public static String logFileName() {
        return "log";
    }

    public static String tempFileName(
        String extension
    ) {
        UUID id = UUID.randomUUID();
        String name = id.toString() + "_" + "tmp" + extension;
        return name;
    }

    public static String committedFileName(
        String tableName,
        TopicPartition topicPart,
        long startOffset,
        long endOffset,
        String extension,
        String zeroPadFormat
    ) {

        int partition = topicPart.partition();
        StringBuilder sb = new StringBuilder();
        sb.append(tableName);
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(partition);
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, startOffset));
        sb.append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, endOffset));
        sb.append(extension);
        String name = sb.toString();
        return name;
    }

    public static FileStatus fileStatusWithMaxOffset(
        HdfsStorage storage,
        Path path,
        CommittedFileFilter filter
    ) {
        if (!storage.exists(path.toString())) {
            return null;
        }
        long maxOffset = -1L;
        FileStatus fileStatusWithMaxOffset = null;
        List<FileStatus> statuses = storage.list(path.toString());
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                FileStatus fileStatus = fileStatusWithMaxOffset(storage, status.getPath(), filter);
                if (fileStatus != null) {
                    long offset = extractOffset(fileStatus.getPath().getName());
                    if (offset > maxOffset) {
                        maxOffset = offset;
                        fileStatusWithMaxOffset = fileStatus;
                    }
                }
            } else {
                String filename = status.getPath().getName();
                log.trace("Checked for max offset: {}", status.getPath());
                if (filter.accept(status.getPath())) {
                    long offset = extractOffset(filename);
                    if (offset > maxOffset) {
                        maxOffset = offset;
                        fileStatusWithMaxOffset = status;
                    }
                }
            }
        }
        return fileStatusWithMaxOffset;
    }

    /**
     * Obtain the offset of the last record that was written to the specified HDFS file.
     *
     * @param filename the name of the HDFS file; may not be null
     * @return the offset of the last record written to the specified file in HDFS
     * @throws IllegalArgumentException if the filename does not match the expected pattern
     */
    public static long extractOffset(String filename) {
        Matcher m = HdfsSinkConstants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        // NB: if statement has side effect of enabling group() call
        if (!m.matches()) {
            throw new IllegalArgumentException(filename + " does not match COMMITTED_FILENAME_PATTERN");
        }
        return Long.parseLong(m.group(HdfsSinkConstants.PATTERN_END_OFFSET_GROUP));
    }

    private static ArrayList<FileStatus> getDirectoriesImpl(HdfsStorage storage, Path path) {
        List<FileStatus> statuses = storage.list(path.toString());
        ArrayList<FileStatus> result = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                int count = 0;
                List<FileStatus> fileStatuses = storage.list(status.getPath().toString());
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isDirectory()) {
                        result.addAll(getDirectoriesImpl(storage, fileStatus.getPath()));
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

    public static FileStatus[] getDirectories(HdfsStorage storage, Path path) throws IOException {
        ArrayList<FileStatus> result = getDirectoriesImpl(storage, path);
        return result.toArray(new FileStatus[result.size()]);
    }

    private static ArrayList<FileStatus> traverseImpl(HdfsStorage storage, Path path, PathFilter filter) {
        if (!storage.exists(path.toString())) {
            return new ArrayList<>();
        }
        ArrayList<FileStatus> result = new ArrayList<>();
        List<FileStatus> statuses = storage.list(path.toString());
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                result.addAll(traverseImpl(storage, status.getPath(), filter));
            } else {
                if (filter.accept(status.getPath())) {
                    result.add(status);
                }
            }
        }
        return result;
    }

    private static ArrayList<FileStatus> traverseImpl(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return new ArrayList<>();
        }
        ArrayList<FileStatus> result = new ArrayList<>();
        FileStatus[] statuses = fs.listStatus(path);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                result.addAll(traverseImpl(fs, status.getPath()));
            } else {
                result.add(status);
            }
        }
        return result;
    }

    public static FileStatus[] traverse(HdfsStorage storage, Path path, PathFilter filter)
        throws IOException {
        ArrayList<FileStatus> result = traverseImpl(storage, path, filter);
        return result.toArray(new FileStatus[result.size()]);
    }

    public static FileStatus[] traverse(FileSystem fs, Path path) throws IOException {
        ArrayList<FileStatus> result = traverseImpl(fs, path);
        return result.toArray(new FileStatus[result.size()]);
    }

    /**
     * Repair path start or end  with "/",replace empty
     */
    public static String repairPath(String path) {
        if (Strings.isNullOrEmpty(path)) {
            throw new IllegalArgumentException("Invalided path: " + path);
        }
        return path.replaceAll(PATH_REPAIR_REG, "");
    }

    public static String createPathWitchUrl(String url, String path) {
        if (Strings.isNullOrEmpty(url) || Strings.isNullOrEmpty(path)) {
            throw new IllegalArgumentException(
                String.format("invalided parameter url: %s path: %s", url, path)
            );
        }
        return new StringBuilder()
            .append(url)
            .append(PATH_SEPARATOR)
            .append(repairPath(path)).toString();
    }

    public static String jointPath(String basePath, String... subs) {
        FilePathBuilder builder = FilePath.of(basePath);
        for (String path : subs) {
            builder.join(path);
        }
        return builder.build().path();
    }

    private static boolean isUrl(String path) {
        return (Strings.isNullOrEmpty(path) || !path.startsWith("hdfs:")) ? false : true;
    }

    public static Optional<FileStatus> findMaxOffsetFileStatusInPartition(
        String partitionPath,
        HdfsStorage storage,
        TableTpCommittedFileFilter filter
    ) {

        if (!storage.exists(partitionPath)) {
            throw new IllegalArgumentException(String.format("Invalided partition path: %s", partitionPath));
        }

        long maxOffset = -1;
        FileStatus maxOffsetFileStatus = null;
        List<FileStatus> statuses = storage.list(partitionPath);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                throw new IllegalStateException(String.format("In encodePartition dir, exist dir,status is: %s", status));
            }

            String filename = status.getPath().getName();
            if (filter.accept(status.getPath())) {
                long offset = extractOffset(filename);
                if (offset > maxOffset) {
                    maxOffset = offset;
                    maxOffsetFileStatus = status;
                }
            }
        }
        return Optional.ofNullable(maxOffsetFileStatus);
    }

    public static String findMaxOffsetFileInPartition(
        String path,
        HdfsStorage storage,
        TableTpCommittedFileFilter filter

    ) {
        Optional<FileStatus> fileStatusOpt = findMaxOffsetFileStatusInPartition(path, storage, filter);
        return fileStatusOpt.orElseThrow(() -> new IllegalStateException("Not exits file")).getPath().getName();
    }

    public static String committedFileName(
        String tableName,
        String partitionPath,
        TopicPartition topicPart,
        String extension,
        String zeroPadFormat,
        HdfsStorage storage,
        TableTpCommittedFileFilter filter
    ) {

        int partition = topicPart.partition();
        Optional<FileStatus> fileStatus = findMaxOffsetFileStatusInPartition(partitionPath, storage, filter);
        long startOffset = NumberUtils.LONG_ZERO;
        long endOffset = fileStatus.isPresent()
            ? extractOffset(fileStatus.get().getPath().getName())
            : NumberUtils.LONG_ZERO;

        return new StringBuilder()
            .append(tableName)
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(partition)
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(String.format(zeroPadFormat, startOffset))
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(String.format(zeroPadFormat, endOffset + 1))
            .append(extension).toString();
    }


}
