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

import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.FilePath.FilePathBuilder;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.ArrayList;
import java.util.List;


/**
 * Hdfs file utils for unit test.
 *
 * @see cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator
 * @see FilePath
 * @deprecated Instead of FilePath and  HdfsFileOperator.
 */
@Deprecated
public class FileUtils {

    private static final String PATH_SEPARATOR = "/";

    private static final String PATH_REPAIR_REG = "^/|/$";

    /**
     * Get log file name.
     *
     * @return log file name
     */
    public static String logFileName() {
        return "log";
    }

    private static ArrayList<FileStatus> traverseImpl(final HdfsStorage storage, final Path path, final PathFilter filter) {
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

    /**
     * Get file collection with given path.
     *
     * @param storage HDFS storage
     * @param path    file path
     * @param filter  path filter
     * @return file collection
     */
    public static FileStatus[] traverse(final HdfsStorage storage, final Path path, final PathFilter filter) {
        ArrayList<FileStatus> result = traverseImpl(storage, path, filter);
        return result.toArray(new FileStatus[result.size()]);
    }

    /**
     * Repair path start or end  with "/",replace empty.
     *
     * @param path file path
     * @return repaired file path
     */
    public static String repairPath(final String path) {
        if (Strings.isNullOrEmpty(path)) {
            throw new IllegalArgumentException("Invalided path: " + path);
        }
        return path.replaceAll(PATH_REPAIR_REG, "");
    }

    /**
     * Create path with given HDFS url.
     *
     * @param url  hdfs url
     * @param path file path
     * @return file path with HDFS url
     */
    public static String createPathWitchUrl(final String url, final String path) {
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

    /**
     * Join file path.
     *
     * @param basePath base file path
     * @param subs     sub file paths
     * @return full path of file
     */
    public static String jointPath(final String basePath, final String... subs) {
        FilePathBuilder builder = FilePath.of(basePath);
        for (String path : subs) {
            builder.join(path);
        }
        return builder.build().path();
    }
}
