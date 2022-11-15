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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

public class MemoryStorage extends HdfsStorage {

    private static final Map<String, List<Object>> DATA = Data.getData();

    private HdfsSinkConfig conf;

    private String url;

    private Failure failure = Failure.noFailure;

    public enum Failure {
        noFailure,
        listStatusFailure,
        appendFailure,
        mkdirsFailure,
        existsFailure,
        deleteFailure,
        commitFailure,
        closeFailure
    }

    public MemoryStorage(final HdfsSinkConfig conf, final String url) {
        super(conf, url, null);
        this.conf = conf;
        this.url = url;
    }

    @Override
    public List<FileStatus> list(final String path) {
        List<FileStatus> result = new ArrayList<>();
        for (String key : DATA.keySet()) {
            if (key.startsWith(path)) {
                FileStatus status = new FileStatus(DATA.get(key).size(), false, 1, 0, 0, 0, null, null, null, new Path(key));
                result.add(status);
            }
        }
        return result;
    }

    /**
     * Get file list.
     * @param path base path
     * @param filter file filter
     * @return FileStatus list
     */
    public List<FileStatus> list(final String path, final PathFilter filter) {
        if (failure == Failure.listStatusFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("listStatus failed.");
        }
        List<FileStatus> result = new ArrayList<>();
        for (String key : DATA.keySet()) {
            if (key.startsWith(path) && filter.accept(new Path(key))) {
                FileStatus status = new FileStatus(DATA.get(key).size(), false, 1, 0, 0, 0, null, null, null, new Path(key));
                result.add(status);
            }
        }
        return result;
    }

    /**
     * Append data to file.
     * @param filename  filename
     * @param object  append value
     */
    public void append(final String filename, final Object object) {
        if (failure == Failure.appendFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("append failed.");
        }
        if (!DATA.containsKey(filename)) {
            DATA.put(filename, new LinkedList<>());
        }
        DATA.get(filename).add(object);
    }

    @Override
    public boolean create(final String filename) {
        if (failure == Failure.mkdirsFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("mkdirs failed.");
        }
        return true;
    }

    @Override
    public boolean exists(final String filename) {
        if (failure == Failure.existsFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("exists failed.");
        }
        return DATA.containsKey(filename);
    }

    @Override
    public void delete(final String filename) {
        if (failure == Failure.deleteFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("delete failed.");
        }
        if (DATA.containsKey(filename)) {
            DATA.get(filename).clear();
            DATA.remove(filename);
        }
    }

    @Override
    public void commit(final String tempFile, final String committedFile) {
        if (failure == Failure.commitFailure) {
            failure = Failure.noFailure;
        }
        if (!DATA.containsKey(committedFile)) {
            List<Object> entryList = DATA.get(tempFile);
            DATA.put(committedFile, entryList);
            DATA.remove(tempFile);
        }
    }

    @Override
    public void close() {
        if (failure == Failure.closeFailure) {
            failure = Failure.noFailure;
            throw new ConnectException("close failed.");
        }
        DATA.clear();
    }

    @Override
    public WAL wal(final StoreConfig storeConfig, final TopicPartition topicPart) {
        return new MemoryWAL(storeConfig, topicPart, this);
    }

    @Override
    public HdfsSinkConfig conf() {
        return conf;
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public SeekableInput open(final String filename, final HdfsSinkConfig conf) {
        return null;
    }
}
