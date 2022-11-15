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

import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.wal.FilePathOffset;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;

public class MemoryWAL implements WAL {

    private static final Map<String, List<Object>> DATA = Data.getData();

    private String logFile;

    private MemoryStorage storage;

    public MemoryWAL(final StoreConfig storeConfig, final TopicPartition topicPart, final MemoryStorage storage)
        throws ConnectException {
        this.storage = storage;
        logFile = FileUtils.jointPath(
            storeConfig.walLogPath(),
            storeConfig.table(),
            String.valueOf(topicPart.partition()),
            FileUtils.logFileName()
        );
    }

    @Override
    public void acquireLease() throws ConnectException {

    }

    @Override
    public void append(final String tempFile, final String committedFile) throws ConnectException {
        LogEntry entry = new LogEntry(tempFile, committedFile);
        storage.append(logFile, entry);
    }

    @Override
    public void apply() throws ConnectException {
        if (DATA.containsKey(logFile)) {
            List<Object> entryList = DATA.get(logFile);
            for (Object entry : entryList) {
                LogEntry logEntry = (LogEntry) entry;
                storage.commit(logEntry.key(), logEntry.value());
            }
        }
    }

    @Override
    public void truncate() throws ConnectException {
        storage.commit(logFile, logFile + ".1");
        storage.delete(logFile);
    }

    @Override
    public void close() throws ConnectException {
        storage.close();
    }

    @Override
    public String getLogFile() {
        return logFile;
    }

    @Override
    public FilePathOffset extractLatestOffset() {
        return null;
    }

    private static class LogEntry {

        private String key;

        private String value;

        LogEntry(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }
}
