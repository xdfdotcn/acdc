package cn.xdf.acdc.connect.core.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.util.Collection;

/**
 * A writer can write records to other data systems.
 */
public interface Writer extends Closeable {

    /**
     * Write records to destinations.
     *
     * @param records records
     * @throws ConnectException connect exception
     */
    void write(Collection<SinkRecord> records) throws ConnectException;
}
