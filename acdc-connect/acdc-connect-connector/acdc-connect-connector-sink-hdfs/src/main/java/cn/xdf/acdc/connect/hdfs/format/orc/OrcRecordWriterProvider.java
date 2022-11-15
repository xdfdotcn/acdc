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

package cn.xdf.acdc.connect.hdfs.format.orc;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.hive.SchemaConverter;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public class OrcRecordWriterProvider implements RecordWriterProvider {

    private static final String EXTENSION = ".orc";

    private final SinkConfig config;

    public OrcRecordWriterProvider(final SinkConfig config) {
        this.config = config;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter newRecordWriter(final String fileName) {
        return new OrcRecordWriter((HdfsSinkConfig) this.config, fileName);
    }

    @Slf4j
    private static class OrcRecordWriter implements RecordWriter {

        private final HdfsSinkConfig conf;

        private final String filename;

        private Writer writer;

        private TypeInfo typeInfo;

        private Schema schema;

        private final Path path;

        OrcRecordWriter(final HdfsSinkConfig conf, final String filename) {
            this.conf = conf;
            this.filename = filename;
            this.path = new Path(filename);
        }

        @Override
        public void write(final SinkRecord record) {
            try {
                if (schema == null) {
                    schema = record.valueSchema();
                    if (schema.type() == Schema.Type.STRUCT) {

                        OrcFile.WriterCallback writerCallback = new OrcFile.WriterCallback() {
                            @Override
                            public void preStripeWrite(final OrcFile.WriterContext writerContext) {
                            }

                            @Override
                            public void preFooterWrite(final OrcFile.WriterContext writerContext) {
                            }
                        };

                        typeInfo = SchemaConverter.convert(schema);
                        ObjectInspector objectInspector = OrcStruct.createObjectInspector(typeInfo);

                        log.info("Opening ORC record writer for: {}", filename);
                        writer = OrcFile
                            .createWriter(path, OrcFile.writerOptions(conf.getHadoopConfiguration())
                                .inspector(objectInspector)
                                .callback(writerCallback));
                    }
                }

                if (schema.type() == Schema.Type.STRUCT) {
                    log.trace(
                        "Writing record from topic {} partition {} offset {}",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset()
                    );

                    Struct struct = (Struct) record.value();
                    OrcStruct row = OrcUtil.createOrcStruct(typeInfo, OrcUtil.convertStruct(struct));
                    writer.addRow(row);

                } else {
                    throw new ConnectException(
                        "Top level type must be STRUCT but was " + schema.type().getName()
                    );
                }
            } catch (IOException e) {
                throw new ConnectException("Failed to write record: ", e);
            }
        }

        @Override
        public void close() {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new ConnectException("Failed to close ORC writer:", e);
            }
        }

        @Override
        public void commit() {

        }
    }
}
