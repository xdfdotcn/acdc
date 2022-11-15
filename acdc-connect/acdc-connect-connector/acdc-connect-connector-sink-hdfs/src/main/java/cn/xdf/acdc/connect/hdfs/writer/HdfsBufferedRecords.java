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

package cn.xdf.acdc.connect.hdfs.writer;

import cn.xdf.acdc.connect.core.sink.AbstractBufferedRecords;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public class HdfsBufferedRecords extends AbstractBufferedRecords {

    private HdfsWriterCoordinator hdfsWriterCoordinator;

    public HdfsBufferedRecords(
        final SinkConfig config,
        final HdfsWriterCoordinator hdfsWriterCoordinator) {
        super(config);
        this.hdfsWriterCoordinator = hdfsWriterCoordinator;
    }

    @Override
    protected void initMetadata(final SinkRecord record) throws ConnectException {
        // TODO 实现schema 表更,进行表结构调整
    }

    @Override
    protected void doFlush(final List<SinkRecord> records) throws ConnectException {
        hdfsWriterCoordinator.write(records);
    }

    @Override
    protected void close() throws RetriableException {

    }
}
