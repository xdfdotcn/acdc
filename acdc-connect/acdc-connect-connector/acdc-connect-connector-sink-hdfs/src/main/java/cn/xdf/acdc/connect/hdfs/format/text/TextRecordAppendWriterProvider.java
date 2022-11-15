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

package cn.xdf.acdc.connect.hdfs.format.text;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public class TextRecordAppendWriterProvider implements RecordWriterProvider {

    private static final String EXTENSION = ".txt";

    private final HdfsFileOperator fileOperator;

    private final StoreConfig storeConfig;

    public TextRecordAppendWriterProvider(
        final HdfsFileOperator fileOperator,
        final StoreConfig storeConfig
    ) {
        this.fileOperator = fileOperator;
        this.storeConfig = storeConfig;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter newRecordWriter(final String fileName) {
        return new TextRecordAppendWriter(fileOperator, storeConfig, fileName);
    }

    @Slf4j
    private static class TextRecordAppendWriter implements RecordWriter {

        private static final int WRITER_BUFFER_SIZE = 128 * 1024;

        private final HdfsFileOperator fileOperator;

        private final StoreConfig storeConfig;

        private final String fileName;

        private final OutputStream out;

        private final OutputStreamWriter streamWriter;

        private final BufferedWriter writer;

        private long fileSize;

        private long writtenCount;

        TextRecordAppendWriter(
            final HdfsFileOperator fileOperator,
            final StoreConfig storeConfig,
            final String fileName
        ) {

            this.fileOperator = fileOperator;
            this.storeConfig = storeConfig;
            this.fileName = fileName;
            // use append mode
            this.out = fileOperator.storage().append(this.fileName);
            this.streamWriter = new OutputStreamWriter(this.out, Charset.forName(HdfsSinkConstants.UTF8_CHARACTER));
            this.writer = new BufferedWriter(this.streamWriter, WRITER_BUFFER_SIZE);
            readFileSize();
            writtenCount = 0L;
        }

        private void readFileSize() {
            FileStatus fileStatus = fileOperator.storage().getFileStatus(fileName);
            if (null != fileStatus) {
                this.fileSize = fileStatus.getLen();
            } else {
                fileSize = 0L;
            }
        }

        @Override
        public void write(final SinkRecord record) {
            try {
                String writeLine = CharMatcher.breakingWhitespace().replaceFrom(getWriteLine(record), TextUtil.BLANK_STRING);
                long byteSize = writeLine.getBytes(StandardCharsets.UTF_8).length;
                writer.write(writeLine);
                writer.newLine();
                fileSize += byteSize;
                writtenCount++;
                if (log.isDebugEnabled()) {
                    log.debug("Write line: {}", writeLine);
                }
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void close() {
            try {
                writer.close();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void commit() {
            try {
                writer.flush();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        private String getWriteLine(final SinkRecord sinkRecord) {
            Struct struct = (Struct) sinkRecord.value();
            List<Object> data = TextUtil.convertStruct(struct);
            return Joiner.on(storeConfig.textSeparator()).join(data);
        }

        @Override
        public long fileSize() {
            return this.fileSize;
        }

        @Override
        public String fileName() {
            return this.fileName;
        }

        @Override
        public long writtenRecordCount() {
            return this.writtenCount;
        }
    }
}
