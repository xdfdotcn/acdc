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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Time;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public final class WALFile {

    private static final Log LOG = LogFactory.getLog(WALFile.class);

    private static final byte INITIAL_VERSION = (byte) 0;

    // "length" of sync entries
    private static final int SYNC_ESCAPE = -1;

    // number of bytes in hash
    private static final int SYNC_HASH_SIZE = 16;

    // escape + hash
    private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE;

    /**
     * The number of bytes between sync points.
     */
    public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

    private static final byte[] VERSION = new byte[] {
        (byte) 'W', (byte) 'A', (byte) 'L', INITIAL_VERSION,
    };

    private static String deserErrorFmt = "Could not find a deserializer for the %s class: '%s'. "
        + "Please ensure that the configuration '%s' is properly configured, if you're using custom"
        + " serialization.";

    private WALFile() {
    }

    /**
     * Create WAL file writer.
     * @param conf  config
     * @param opts  opts set
     * @return WAL file writer
     * @throws IOException failed to create a Writer
     */
    public static Writer createWriter(
        final HdfsSinkConfig conf,
        final Writer.Option... opts
    ) throws IOException {
        return new Writer(conf, opts);
    }

    /**
     * Get the configured buffer size.
     */
    private static int getBufferSize(final Configuration conf) {
        return conf.getInt("io.file.buffer.size", 4096);
    }

    public static class Writer implements Closeable, Syncable {

        private Serializer<WALEntry> keySerializer;

        private Serializer<WALEntry> valSerializer;

        private boolean ownOutputStream = true;

        // Insert a globally unique 16-byte value every few entries, so that one
        // can seek into the middle of a file and then synchronize with record
        // starts and ends by scanning for this value.

        // position of last sync
        private long lastSyncPos;

        // 16 random bytes
        private byte[] sync;

        private FileSystem fs;

        private FSDataOutputStream out;

        private DataOutputBuffer buffer = new DataOutputBuffer();

        private boolean appendMode;

        {
            try {
                MessageDigest digester = MessageDigest.getInstance("MD5");
                long time = Time.now();
                digester.update((new UID() + "@" + time).getBytes(Charsets.UTF_8));
                sync = digester.digest();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        Writer(final HdfsSinkConfig connectorConfig, final Option... opts) throws IOException {
            Configuration conf = connectorConfig.getHadoopConfiguration();
            BlockSizeOption blockSizeOption =
                Options.getOption(BlockSizeOption.class, opts);
            BufferSizeOption bufferSizeOption =
                Options.getOption(BufferSizeOption.class, opts);
            ReplicationOption replicationOption =
                Options.getOption(ReplicationOption.class, opts);

            FileOption fileOption = Options.getOption(FileOption.class, opts);
            AppendIfExistsOption appendIfExistsOption = Options.getOption(
                AppendIfExistsOption.class, opts);
            StreamOption streamOption = Options.getOption(StreamOption.class, opts);

            // check consistency of options
            if ((fileOption == null) == (streamOption == null)) {
                throw new IllegalArgumentException("file or stream must be specified");
            }
            if (fileOption == null && (
                blockSizeOption != null
                    || bufferSizeOption != null
                    || replicationOption != null)
            ) {
                throw new IllegalArgumentException("file modifier options not compatible with stream");
            }

            FSDataOutputStream out;
            boolean ownStream = fileOption != null;

            try {
                if (ownStream) {
                    Path p = fileOption.getValue();
                    fs = FileSystem.newInstance(p.toUri(), conf);
                    int bufferSize = bufferSizeOption == null
                        ? getBufferSize(conf)
                        : bufferSizeOption.getValue();
                    short replication = replicationOption == null
                        ? fs.getDefaultReplication(p)
                        : (short) replicationOption.getValue();
                    long blockSize = blockSizeOption == null
                        ? fs.getDefaultBlockSize(p)
                        : blockSizeOption.getValue();

                    if (appendIfExistsOption != null
                        && appendIfExistsOption.getValue()
                        && fs.exists(p)
                        && hasIntactVersionHeader(p, fs)) {
                        // Read the file and verify header details
                        try (Reader reader = new Reader(
                            connectorConfig.getHadoopConfiguration(),
                            Reader.file(p),
                            new Reader.OnlyHeaderOption()
                        )) {
                            checkVersion(reader.getVersion(), VERSION[3]);
                            sync = reader.getSync();
                        }
                        out = fs.append(p, bufferSize);
                        this.appendMode = true;
                    } else {
                        out = fs.create(p, true, bufferSize, replication, blockSize);
                    }
                } else {
                    out = streamOption.getValue();
                }

                init(connectorConfig, out, ownStream);
            } catch (RemoteException remoteException) {
                log.warn("Failed creating a WAL Writer: " + remoteException.getMessage());
                if (fs != null) {
                    try {
                        fs.close();
                    } catch (IOException t) {
                        log.error("Could not close filesystem", t);
                    }
                }
                throw remoteException;
            }

        }

        private void checkVersion(final byte source, final byte target) throws VersionMismatchException {
            if (source != target) {
                throw new VersionMismatchException(target, source);
            }
        }

        private boolean hasIntactVersionHeader(final Path p, final FileSystem fs) throws IOException {
            FileStatus[] statuses = fs.listStatus(p);
            if (statuses.length != 1) {
                throw new ConnectException("Expected exactly one log for WAL file " + p);
            }
            boolean result = statuses[0].getLen() >= VERSION.length;
            if (!result) {
                log.warn("Failed to read version header from WAL file " + p);
            }
            return result;
        }

        protected Serializer<WALEntry> getKeySerializer() {
            return keySerializer;
        }

        protected Serializer<WALEntry> getValSerializer() {
            return valSerializer;
        }

        protected boolean isOwnOutputStream() {
            return ownOutputStream;
        }

        protected long getLastSyncPos() {
            return lastSyncPos;
        }

        protected byte[] getSync() {
            return sync;
        }

        /**
         * Sync data.
         * @param sync byte arrays that need to be synchronized
         */
        public void setSync(final byte[] sync) {
            this.sync = sync;
        }

        protected FileSystem getFs() {
            return fs;
        }

        protected FSDataOutputStream getOut() {
            return out;
        }

        protected DataOutputBuffer getBuffer() {
            return buffer;
        }

        protected boolean isAppendMode() {
            return appendMode;
        }

        /**
         * Get file option.
         * @param value file path
         * @return option
         */
        public static Option file(final Path value) {
            return new FileOption(value);
        }

        /**
         * Append if it already exists.
         * @param value set config
         * @return option
         */
        public static Option appendIfExists(boolean value) {
            return new AppendIfExistsOption(value);
        }

        void init(final HdfsSinkConfig connectorConfig, final FSDataOutputStream out, boolean ownStream)
            throws IOException {
            Configuration conf = connectorConfig.getHadoopConfiguration();
            this.out = out;
            this.ownOutputStream = ownStream;
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            this.keySerializer = serializationFactory.getSerializer(WALEntry.class);
            if (this.keySerializer == null) {
                String errorMsg = String.format(
                    deserErrorFmt,
                    "Key",
                    WALEntry.class.getCanonicalName(),
                    CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                );
                throw new IOException(errorMsg);
            }
            this.keySerializer.open(buffer);
            this.valSerializer = serializationFactory.getSerializer(WALEntry.class);
            if (this.valSerializer == null) {
                String errorMsg = String.format(
                    deserErrorFmt,
                    "Value",
                    WALEntry.class.getCanonicalName(),
                    CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                );
                throw new IOException(errorMsg);
            }
            this.valSerializer.open(buffer);
            if (appendMode) {
                sync();
            } else {
                writeFileHeader();
            }
        }

        /**
         * Append value to WAL log.
         * @param key key
         * @param val  value
         * @throws IOException append fail exception
         */
        public synchronized void append(final WALEntry key, final WALEntry val)
            throws IOException {
            buffer.reset();

            // Append the 'key'
            keySerializer.serialize(key);
            int keyLength = buffer.getLength();
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }

            valSerializer.serialize(val);

            // Write the record out

            // sync
            checkAndWriteSync();
            // total record length
            out.writeInt(buffer.getLength());
            // key portion length
            out.writeInt(keyLength);
            // data
            out.write(buffer.getData(), 0, buffer.getLength());
        }

        /**
         * Returns the current length of the output file.
         *
         * <p>This always returns a synchronized position.  In other words, immediately after calling
         * {@link Reader#seek(long)} with a position returned by this method, {@link
         * Reader#next(Writable)} may be called.  However the key may be earlier in the file
         * than key last written when this method was called (e.g., with block-compression, it may be
         * the first key in the block that was being written when this method was called).
         *
         * @return the current length of the output file.
         * @throws IOException Exception on getting position
         */
        public synchronized long getLength() throws IOException {
            return out.getPos();
        }

        private synchronized void checkAndWriteSync() throws IOException {
            if (sync != null && out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
                // time to emit sync
                sync();
            }
        }

        private void writeFileHeader()
            throws IOException {
            // write the version
            out.write(VERSION);
            // write the sync bytes
            out.write(sync);
            // flush header
            out.flush();
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                keySerializer.close();
                valSerializer.close();
                if (out != null) {
                    // Close the underlying stream iff we own it...
                    if (ownOutputStream) {
                        out.close();
                    } else {
                        out.flush();
                    }

                }
            } finally {
                if (fs != null) {
                    try {
                        fs.close();
                    } catch (IOException t) {
                        log.error("Could not close FileSystem", t);
                    }
                }
                out = null;
            }
        }

        @Override
        public void sync() throws IOException {
            if (sync != null && lastSyncPos != out.getPos()) {
                // mark the start of the sync
                out.writeInt(SYNC_ESCAPE);
                // write sync
                out.write(sync);
                // update lastSyncPos
                lastSyncPos = out.getPos();
            }
        }

        @Override
        public void hflush() throws IOException {
            if (out != null) {
                out.hflush();
            }
        }

        @Override
        public void hsync() throws IOException {
            if (out != null) {
                out.hsync();
            }
        }

        /**
         * A tag interface for all of the Reader options.
         */
        public interface Option {

        }

        static class FileOption extends Options.PathOption implements Option {

            FileOption(final Path path) {
                super(path);
            }
        }

        static class StreamOption extends Options.FSDataOutputStreamOption
            implements Option {

            StreamOption(final FSDataOutputStream stream) {
                super(stream);
            }
        }

        static class BufferSizeOption extends Options.IntegerOption
            implements Option {

            BufferSizeOption(int value) {
                super(value);
            }
        }

        static class BlockSizeOption extends Options.LongOption implements Option {

            BlockSizeOption(long value) {
                super(value);
            }
        }

        static class ReplicationOption extends Options.IntegerOption
            implements Option {

            ReplicationOption(int value) {
                super(value);
            }
        }

        static class AppendIfExistsOption extends Options.BooleanOption implements Option {

            AppendIfExistsOption(boolean value) {
                super(value);
            }
        }

    }

    public static class Reader implements Closeable {

        private String filename;

        private FileSystem fs;

        private FSDataInputStream in;

        private DataOutputBuffer outBuf = new DataOutputBuffer();

        private byte version;

        private byte[] sync = new byte[SYNC_HASH_SIZE];

        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];

        private boolean syncSeen;

        private long headerEnd;

        private long end;

        private int keyLength;

        private int recordLength;

        private Configuration conf;

        private DataInputBuffer valBuffer;

        private DataInputStream valIn;

        private Deserializer<WALEntry> keyDeserializer;

        private Deserializer<WALEntry> valDeserializer;

        public Reader(final Configuration conf, final Option... opts) throws IOException {
            // Look up the options, these are null if not set
            FileOption fileOpt = Options.getOption(FileOption.class, opts);
            InputStreamOption streamOpt = Options.getOption(InputStreamOption.class, opts);
            LengthOption lenOpt = Options.getOption(LengthOption.class, opts);
            BufferSizeOption bufOpt = Options.getOption(BufferSizeOption.class, opts);

            // check for consistency
            if ((fileOpt == null) == (streamOpt == null)) {
                throw new
                    IllegalArgumentException("File or stream option must be specified");
            }
            if (fileOpt == null && bufOpt != null) {
                throw new IllegalArgumentException("buffer size can only be set when a file is specified.");
            }

            // figure out the real values
            Path filename = null;
            FSDataInputStream file;
            final long len;

            try {
                if (fileOpt != null) {
                    filename = fileOpt.getValue();
                    fs = FileSystem.newInstance(filename.toUri(), conf);
                    int bufSize = bufOpt == null ? getBufferSize(conf) : bufOpt.getValue();
                    len = null == lenOpt
                        ? fs.getFileStatus(filename).getLen()
                        : lenOpt.getValue();
                    file = openFile(fs, filename, bufSize, len);
                } else {
                    len = null == lenOpt ? Long.MAX_VALUE : lenOpt.getValue();
                    file = streamOpt.getValue();
                }
                StartOption startOpt = Options.getOption(StartOption.class, opts);
                long start = startOpt == null ? 0 : startOpt.getValue();
                // really set up
                OnlyHeaderOption headerOnly = Options.getOption(OnlyHeaderOption.class, opts);
                initialize(filename, file, start, len, conf, headerOnly != null);
            } catch (RemoteException remoteException) {
                log.error("Failed creating a WAL Reader: " + remoteException.getMessage());
                if (fs != null) {
                    try {
                        fs.close();
                    } catch (IOException t) {
                        log.error("Error closing FileSystem", t);
                    }
                }
                throw remoteException;
            }
        }

        /**
         * Create an option to specify the path name of the sequence file.
         *
         * @param value the path to read
         * @return a new option
         */
        public static Option file(final Path value) {
            return new FileOption(value);
        }

        /**
         * Create an option to specify the stream with the sequence file.
         *
         * @param value the stream to read.
         * @return a new option
         */
        public static Option stream(final FSDataInputStream value) {
            return new InputStreamOption(value);
        }

        /**
         * Create an option to specify the starting byte to read.
         *
         * @param value the number of bytes to skip over
         * @return a new option
         */
        public static Option start(long value) {
            return new StartOption(value);
        }

        /**
         * Create an option to specify the number of bytes to read.
         *
         * @param value the number of bytes to read
         * @return a new option
         */
        public static Option length(long value) {
            return new LengthOption(value);
        }

        /**
         * Create an option with the buffer size for reading the given pathname.
         *
         * @param value the number of bytes to buffer
         * @return a new option
         */
        public static Option bufferSize(int value) {
            return new BufferSizeOption(value);
        }

        /**
         * Common work of the constructors.
         */
        private void initialize(
            final Path filename, final FSDataInputStream in,
            long start, long length, final Configuration conf,
            boolean tempReader
        ) throws IOException {
            if (in == null) {
                throw new IllegalArgumentException("in == null");
            }
            this.filename = filename == null ? "<unknown>" : filename.toString();
            this.in = in;
            this.conf = conf;
            boolean succeeded = false;
            try {
                seek(start);
                this.end = this.in.getPos() + length;
                // if it wrapped around, use the max
                if (end < length) {
                    end = Long.MAX_VALUE;
                }
                init(tempReader);
                succeeded = true;
            } finally {
                if (!succeeded) {
                    IOUtils.cleanup(LOG, this.in);
                }
            }
        }

        /**
         * Override this method to specialize the type of {@link FSDataInputStream} returned.
         *
         * @param fs The file system used to open the file.
         * @param file The file being read.
         * @param bufferSize The buffer size used to read the file.
         * @param length The length being read if it is equal to or greater than 0.
         *               Otherwise, the length is not available.
         * @return The opened stream.
         * @throws IOException Exception on opening file.
         */
        protected FSDataInputStream openFile(
            final FileSystem fs, final Path file,
            int bufferSize, long length
        ) throws IOException {
            return fs.open(file, bufferSize);
        }

        /**
         * Initialize the {@link Reader}.
         *
         * @param tempReader <code>true</code> if we are constructing a temporary and hence do not
         *      initialize every component; <code>false</code> otherwise.
         * @throws IOException Exception on opening file.
         */
        private void init(boolean tempReader) throws IOException {
            byte[] versionBlock = new byte[VERSION.length];
            in.readFully(versionBlock);

            if ((versionBlock[0] != VERSION[0])
                || (versionBlock[1] != VERSION[1])
                || (versionBlock[2] != VERSION[2])
            ) {
                throw new IOException(this + " not a WALFile");
            }

            // Set 'version'
            version = versionBlock[3];
            if (version > VERSION[3]) {
                throw new VersionMismatchException(VERSION[3], version);
            }

            // read sync bytes
            in.readFully(sync);
            // record end of header
            headerEnd = in.getPos();

            // Initialize... *not* if this we are constructing a temporary Reader
            if (!tempReader) {
                valBuffer = new DataInputBuffer();
                valIn = valBuffer;

                SerializationFactory serializationFactory =
                    new SerializationFactory(conf);
                this.keyDeserializer =
                    getDeserializer(serializationFactory, WALEntry.class);
                if (this.keyDeserializer == null) {
                    String errorMsg = String.format(
                        deserErrorFmt,
                        "Key",
                        WALEntry.class.getCanonicalName(),
                        CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                    );
                    throw new IOException(errorMsg);
                }

                this.keyDeserializer.open(valBuffer);

                this.valDeserializer =
                    getDeserializer(serializationFactory, WALEntry.class);
                if (this.valDeserializer == null) {
                    String errorMsg = String.format(
                        deserErrorFmt,
                        "Value",
                        WALEntry.class.getCanonicalName(),
                        CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                    );
                    throw new IOException(errorMsg);
                }
                this.valDeserializer.open(valIn);
            }
        }

        private <T> Deserializer<T> getDeserializer(final SerializationFactory sf, final Class<T> c) {
            return sf.getDeserializer(c);
        }

        private byte[] getSync() {
            return sync;
        }

        /**
         * Close the file.
         */
        @Override
        public synchronized void close() throws IOException {
            try {
                if (keyDeserializer != null) {
                    keyDeserializer.close();
                }
                if (valDeserializer != null) {
                    valDeserializer.close();
                }

                // Close the input-stream
                in.close();
            } finally {
                try {
                    fs.close();
                } catch (IOException t) {
                    log.error("Unable to close FileSystem", t);
                }
            }
        }

        private byte getVersion() {
            return version;
        }

        /**
         * Returns the configuration used for this file.
         */
        Configuration getConf() {
            return conf;
        }

        /**
         * Position valLenIn/valIn to the 'value' corresponding to the 'current' key.
         */
        private synchronized void seekToCurrentValue() throws IOException {
            valBuffer.reset();
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         * @throws IOException Exception on reading key.
         */
        public synchronized void getCurrentValue(final Writable val)
            throws IOException {
            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }
            // Position stream to 'current' value
            seekToCurrentValue();

            val.readFields(valIn);
            if (valIn.read() > 0) {
                log.info("available bytes: " + valIn.available());
                throw new IOException(
                    val
                        + " read "
                        + (valBuffer.getPosition() - keyLength)
                        + " bytes, should read "
                        + (valBuffer.getLength() - keyLength)
                );
            }
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param walEntry : The 'value' to be read.
         * @return the value corresponding to the last read key.
         * @throws IOException Exception on reading key.
         */
        public synchronized WALEntry getCurrentValue(final WALEntry walEntry)
            throws IOException {
            WALEntry val = walEntry;

            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }

            // Position stream to 'current' value
            seekToCurrentValue();
            val = deserializeValue(val);
            if (valIn.read() > 0) {
                log.info("available bytes: " + valIn.available());
                throw new IOException(
                    val
                        + " read "
                        + (valBuffer.getPosition() - keyLength)
                        + " bytes, should read "
                        + (valBuffer.getLength() - keyLength)
                );
            }
            return val;

        }

        private WALEntry deserializeValue(final WALEntry val) throws IOException {
            return valDeserializer.deserialize(val);
        }

        /**
         * Read and return the next record length, potentially skipping over a sync block.
         *
         * @return the length of the next record or -1 if there is no next record
         */
        private synchronized int readRecordLength() throws IOException {
            if (in.getPos() >= end) {
                return -1;
            }
            int length = in.readInt();
            if (sync != null && length == SYNC_ESCAPE) {
                // process a sync entry
                // read syncCheck
                in.readFully(syncCheck);
                if (!Arrays.equals(sync, syncCheck)) {
                    // check it
                    throw new CorruptWalFileException("File is corrupt!");
                }
                syncSeen = true;
                if (in.getPos() >= end) {
                    return -1;
                }
                // re-read length
                length = in.readInt();
            } else {
                syncSeen = false;
            }

            return length;
        }

        /**
         * Read the next key in the file into <code>key</code>, skipping its value.  True if another
         * entry exists, and false at end of file.
         *
         * @param key the writable to read the key into
         * @return whether another key exists after reading a key
         * @throws IOException Exception on reading key.
         */
        public synchronized boolean next(final Writable key) throws IOException {
            if (key.getClass() != WALEntry.class) {
                throw new IOException("wrong key class: " + key.getClass().getName()
                    + " is not " + WALEntry.class);
            }

            outBuf.reset();

            keyLength = next(outBuf);
            if (keyLength < 0) {
                return false;
            }

            valBuffer.reset(outBuf.getData(), outBuf.getLength());

            key.readFields(valBuffer);
            valBuffer.mark(0);
            if (valBuffer.getPosition() != keyLength) {
                throw new IOException(key + " read " + valBuffer.getPosition()
                    + " bytes, should read " + keyLength);
            }

            return true;
        }

        /**
         * Read the next key/value pair in the file into <code>key</code> and <code>val</code>.  Returns
         * true if such a pair exists and false when at end of file
         *
         * @param key the writable to read the key into
         * @param val the writable to read the val into
         * @return whether another key value pair exists after reading a key
         * @throws IOException Exception on reading key pair.
         */
        public synchronized boolean next(final Writable key, final Writable val) throws IOException {
            if (val.getClass() != WALEntry.class) {
                throw new IOException("wrong value class: " + val + " is not " + WALEntry.class);
            }

            boolean more = next(key);

            if (more) {
                getCurrentValue(val);
            }

            return more;
        }

        /**
         * Read the next key/value pair in the file into <code>buffer</code>. Returns the length of the
         * key read, or -1 if at end of file.  The length of the value may be computed by calling
         * buffer.getLength() before and after calls to this method.
         */
        synchronized int next(final DataOutputBuffer buffer) throws IOException {
            try {
                int length = readRecordLength();
                if (length == -1) {
                    return -1;
                }
                int keyLength = in.readInt();
                buffer.write(in, length);
                return keyLength;
                // checksum failure
            } catch (ChecksumException e) {
                handleChecksumException(e);
                return next(buffer);
            }
        }

        /**
         * Read the next key in the file, skipping its value.  Return null at end of file.
         *
         * @param walEntry the current WALEntry
         * @return null at the end of file
         * @throws IOException Exception on reading key.
         */
        public synchronized WALEntry next(final WALEntry walEntry) throws IOException {
            WALEntry key = walEntry;
            outBuf.reset();
            keyLength = next(outBuf);
            if (keyLength < 0) {
                return null;
            }
            valBuffer.reset(outBuf.getData(), outBuf.getLength());
            key = deserializeKey(key);
            valBuffer.mark(0);
            if (valBuffer.getPosition() != keyLength) {
                throw new IOException(key + " read " + valBuffer.getPosition()
                    + " bytes, should read " + keyLength);
            }
            return key;
        }

        private WALEntry deserializeKey(final WALEntry key) throws IOException {
            return keyDeserializer.deserialize(key);
        }

        private void handleChecksumException(final ChecksumException e)
            throws IOException {
            if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
                log.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
                sync(getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
            } else {
                throw e;
            }
        }

        /**
         * disables sync. often invoked for tmp files
         */
        synchronized void ignoreSync() {
            sync = null;
        }

        /**
         * Set the current byte position in the input file.
         *
         * <p>The position passed must be a position returned by {@link Writer#getLength()} when
         * writing this file.  To seek to an arbitrary position, use {@link Reader#sync(long)}.
         *
         * @param position a position returned by {@link Writer#getLength()} whem
         *                 writing this file.
         * @throws IOException Exception on setting byte position
         */
        public synchronized void seek(long position) throws IOException {
            in.seek(position);
        }

        /**
         * Seek to the start of the file after the header.
         *
         * @throws IOException if unable to seek to the end of the header
         */
        public void seekToFirstRecord() throws IOException {
            in.seek(headerEnd);
        }

        /**
         * Seek to the next sync mark past a given position.
         *
         * @param position sync mark will be found past the given position.
         * @throws IOException Exception on setting byte position.
         */
        public synchronized void sync(long position) throws IOException {
            if (position + SYNC_SIZE >= end) {
                seek(end);
                return;
            }

            if (position < headerEnd) {
                // seek directly to first record
                in.seek(headerEnd);
                // note the sync marker "seen" in the header
                syncSeen = true;
                return;
            }

            try {
                // skip escape
                seek(position + 4);
                in.readFully(syncCheck);
                int syncLen = sync.length;
                for (int i = 0; in.getPos() < end; i++) {
                    int j = 0;
                    for (; j < syncLen; j++) {
                        if (sync[j] != syncCheck[(i + j) % syncLen]) {
                            break;
                        }
                    }
                    if (j == syncLen) {
                        // position before sync
                        in.seek(in.getPos() - SYNC_SIZE);
                        return;
                    }
                    syncCheck[i % syncLen] = in.readByte();
                }
            } catch (ChecksumException e) {
                // checksum failure
                handleChecksumException(e);
            }
        }

        /**
         * Returns true iff the previous call to next passed a sync mark.
         * @return true iff the previous call to next passed a sync mark.
         */
        public synchronized boolean syncSeen() {
            return syncSeen;
        }

        /**
         * Return the current byte position in the input file.
         * @return the current byte position in the input file.
         * @throws IOException Exception on getting position.
         */
        public synchronized long getPosition() throws IOException {
            return in.getPos();
        }

        /**
         * Returns the name of the file.
         * @return the name of the file
         */
        @Override
        public String toString() {
            return filename;
        }

        /**
         * A tag interface for all of the Reader options.
         */
        public interface Option {

        }

        private static final class FileOption extends Options.PathOption implements Option {

            private FileOption(final Path value) {
                super(value);
            }
        }

        private static final class InputStreamOption extends Options.FSDataInputStreamOption implements Option {

            private InputStreamOption(final FSDataInputStream value) {
                super(value);
            }
        }

        private static final class StartOption extends Options.LongOption implements Option {

            private StartOption(long value) {
                super(value);
            }
        }

        private static final class LengthOption extends Options.LongOption implements Option {

            private LengthOption(long value) {
                super(value);
            }
        }

        private static final class BufferSizeOption extends Options.IntegerOption implements Option {

            private BufferSizeOption(int value) {
                super(value);
            }
        }

        // only used directly
        private static final class OnlyHeaderOption extends Options.BooleanOption implements Option {

            private OnlyHeaderOption() {
                super(true);
            }
        }
    }
}
