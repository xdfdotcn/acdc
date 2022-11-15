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

/**
 * Storage specific RecordWriter.
 */
public interface RecordWriterProvider {

    /**
     * The file suffix.
     * @return The file extension.
     */
    String getExtension();

    /**
     * New a RecordWriter.
     * @param fileName file name.
     * @return record write
     */
    RecordWriter newRecordWriter(String fileName);

}
