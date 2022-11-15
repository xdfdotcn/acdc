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

package cn.xdf.acdc.connect.hdfs.format.avro;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import java.io.OutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class AvroRecordWriterProvider implements RecordWriterProvider {

    private static final String EXTENSION = ".avro";

    private final HdfsFileOperator fileOperator;

    private final SinkConfig config;

    public AvroRecordWriterProvider(
        final HdfsFileOperator fileOperator,
        final SinkConfig config
    ) {
        this.fileOperator = fileOperator;
        this.config = config;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter newRecordWriter(final String fileName) {
        return new AvroRecordWriter(fileOperator.storage(), (HdfsSinkConfig) this.config, fileName);
    }

    @Slf4j
    private static class AvroRecordWriter implements RecordWriter {

        private final HdfsStorage storage;

        private final AvroData avroData;

        private final HdfsSinkConfig conf;

        private final String filename;

        private final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());

        private Schema schema;

        AvroRecordWriter(
            final HdfsStorage storage,
            final HdfsSinkConfig conf,
            final String filename) {
            this.storage = storage;
            this.conf = conf;
            this.filename = filename;
            this.avroData = new AvroData(storage.conf().avroDataConfig());
        }

        @Override
        public void write(final SinkRecord record) {
            if (schema == null) {
                schema = record.valueSchema();
                try {
                    log.info("Opening record writer for: {}", filename);
                    final OutputStream out = storage.create(filename, true);
                    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
                    writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
                    writer.create(avroSchema, out);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            log.trace("Sink record: {}", record);
            Object value = avroData.fromConnectData(schema, record.value());
            try {
                // AvroData wraps primitive types so their schema can be included. We need to unwrap
                // NonRecordContainers to just their value to properly handle these types
                if (value instanceof NonRecordContainer) {
                    writer.append(((NonRecordContainer) value).getValue());
                } else {
                    writer.append(value);
                }
            } catch (IOException e) {
                throw new DataException(e);
            }
        }

        @Override
        public void close() {
            try {
                writer.close();
            } catch (IOException e) {
                throw new DataException(e);
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

        @Override
        public String fileName() {
            return null;
        }
    }
}
