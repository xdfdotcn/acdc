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

package cn.xdf.acdc.connect.hdfs.format.parquet;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetRecordWriterProvider implements RecordWriterProvider {

    private static final String EXTENSION = ".parquet";

    private final HdfsFileOperator fileOperator;

    private final SinkConfig config;

    public ParquetRecordWriterProvider(
        final HdfsFileOperator fileOperator,
        final SinkConfig config) {
        this.fileOperator = fileOperator;
        this.config = config;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter newRecordWriter(final String fileName) {
        return new ParquetRecordWriter(this.fileOperator, (HdfsSinkConfig) this.config, fileName);
    }

    @Slf4j
    private static class ParquetRecordWriter implements RecordWriter {

        private final AvroData avroData;

        private final HdfsFileOperator fileOperator;

        private final HdfsSinkConfig conf;

        private final String filename;

        private final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        private final int blockSize = 256 * 1024 * 1024;

        private final int pageSize = 64 * 1024;

        private final Path path;

        private Schema schema;

        private ParquetWriter<GenericRecord> writer;

        ParquetRecordWriter(
            final HdfsFileOperator fileOperator,
            final HdfsSinkConfig conf,
            final String filename) {
            this.fileOperator = fileOperator;
            this.conf = conf;
            this.filename = filename;
            this.avroData = new AvroData(fileOperator.storage().conf().avroDataConfig());
            this.path = new Path(this.filename);
        }

        @Override
        public void write(final SinkRecord record) {
            if (schema == null) {
                schema = record.valueSchema();
                // may still be null at this point
            }

            if (writer == null) {
                try {
                    log.info("Opening record writer for: {}", filename);
                    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
                    writer = AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(avroSchema)
                        .withCompressionCodec(compressionCodecName)
                        .withRowGroupSize(blockSize)
                        .withPageSize(pageSize)
                        .withDictionaryEncoding(true)
                        .withConf(conf.getHadoopConfiguration())
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build();
                    log.debug("Opened record writer for: {}", filename);
                } catch (IOException e) {
                    // Ultimately caught and logged in TopicPartitionWriter,
                    // but log in debug to provide more context
                    log.warn(
                        "Error creating {} for file '{}', {}, and schema {}: ",
                        AvroParquetWriter.class.getSimpleName(),
                        filename,
                        compressionCodecName,
                        schema,
                        e
                    );
                    throw new ConnectException(e);
                }
            }

            log.trace("Sink record: {}", record);
            Object value = avroData.fromConnectData(record.valueSchema(), record.value());
            try {
                writer.write((GenericRecord) value);
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        }

        @Override
        public void commit() {

        }
    }
}
