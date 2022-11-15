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

package cn.xdf.acdc.connect.jdbc.sink;

import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialects;
import cn.xdf.acdc.connect.jdbc.util.Version;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Slf4j
@Getter
@Setter
public class JdbcSinkTask extends SinkTask {

    private ErrantRecordReporter reporter;

    private DatabaseDialect dialect;

    private JdbcSinkConfig config;

    private JdbcDbWriter writer;

    private int remainingRetries;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initWriter();
        remainingRetries = config.getMaxRetries();
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Will occur in Connect runtimes earlier than 2.6
            reporter = null;
        }
    }

    void initWriter() {
        if (config.getDialectName() != null && !config.getDialectName().trim().isEmpty()) {
            dialect = DatabaseDialects.create(config.getDialectName(), config);
        } else {
            dialect = DatabaseDialects.findBestFor(config.getConnectionUrl(), config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (RetriableException e) {
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries,
                    e
            );
            if (remainingRetries > 0) {
                writer.close();
                initWriter();
                remainingRetries--;
                context.timeout(config.getRetryBackoffMs());
                throw e;
            } else {
                if (reporter != null) {
                    unrollAndRetry(records);
                } else {
                    log.error("Failing task after exhausting retries", e);
                    throw new ConnectException(e);
                }
            }
        } catch (ConnectException e) {
            if (reporter != null) {
                unrollAndRetry(records);
            } else {
                throw e;
            }
        }
        remainingRetries = config.getMaxRetries();
    }

    private void unrollAndRetry(final Collection<SinkRecord> records) {
        try {
            writer.close();
        } finally {
            for (SinkRecord record : records) {
                try {
                    writer.write(Collections.singletonList(record));
                } catch (ConnectException e) {
                    reporter.report(record, e);
                    writer.close();
                }
            }
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    /**
     * Stop the JDBC db writer.
     */
    public void stop() {
        log.info("Stopping task");
        try {
            writer.close();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

}
