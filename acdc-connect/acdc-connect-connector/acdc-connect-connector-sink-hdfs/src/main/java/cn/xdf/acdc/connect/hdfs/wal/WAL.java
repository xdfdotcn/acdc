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

package cn.xdf.acdc.connect.hdfs.wal;

import org.apache.kafka.connect.errors.ConnectException;

public interface WAL {

    // CHECKSTYLE:OFF
    String beginMarker = "BEGIN";

    String endMarker = "END";
    // CHECKSTYLE:ON

    /**
     * Get file Lease.
     * @throws ConnectException exception on get lease
     */
    void acquireLease() throws ConnectException;

    /**
     * Append WAL log.
     * @param tempFile  temp file
     * @param committedFile  commit file
     * @throws ConnectException append fail exception
     */
    void append(String tempFile, String committedFile) throws ConnectException;

    /**
     * Apply WAL log.
     * @throws ConnectException apply fail exception
     * */
    void apply() throws ConnectException;

    /**
     * Truncate WAL log.
     * @throws ConnectException apply fail exception
     */
    void truncate() throws ConnectException;

    /**
     * Close WAL log.
     * @throws ConnectException close fail exception
     */
    void close() throws ConnectException;

    /**
     * Get WAL log file .
     * @return log file name
     */
    String getLogFile();

    /**
     * Extract offset from WAL log.
     * @return file path and offset
     */
    FilePathOffset extractLatestOffset();
}
