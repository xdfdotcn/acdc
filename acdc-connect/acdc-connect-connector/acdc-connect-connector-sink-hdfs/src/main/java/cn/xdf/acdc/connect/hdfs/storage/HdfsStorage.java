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

package cn.xdf.acdc.connect.hdfs.storage;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.wal.FSWAL;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public class HdfsStorage {

    private final FileSystem fs;

    private final HdfsSinkConfig conf;

    private final String url;

    // Visible for testing.
    protected HdfsStorage(
        final HdfsSinkConfig conf,
        final String url,
        final FileSystem fs) {
        this.conf = conf;
        this.url = url;
        this.fs = fs;
    }

    public HdfsStorage(final HdfsSinkConfig conf,
        final String url) throws IOException {
        this.conf = conf;
        this.url = url;
        fs = FileSystem.newInstance(URI.create(url), conf.getHadoopConfiguration());
    }

    /**
     * List the contents of the storage at a given path.
     *
     * <p>In stores supporting weak consistency models (e.g. eventual consistency) the result of this
     * operation might not be correspond to the most recent state of the store. For instance, created
     * files might not show up, deleted files might still be listed.
     *
     * @param path the path.
     * @param filter path filter
     * @return the listing of the contents.
     */
    public List<FileStatus> list(final String path, final PathFilter filter) {
        try {
            return Arrays.asList(fs.listStatus(new Path(path), filter));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * List the contents of the storage at a given path.
     *
     * <p>In stores supporting weak consistency models (e.g. eventual consistency) the result of this
     * operation might not be correspond to the most recent state of the store. For instance, created
     * files might not show up, deleted files might still be listed.
     *
     * @param path the path.
     * @return the listing of the contents.
     */
    public List<FileStatus> list(final String path) {
        try {
            return Arrays.asList(fs.listStatus(new Path(path)));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Append data to an existing object at the given path (optional operation).
     * @param filename the path of the object to be appended.
     * @return an output stream associated with the existing object.
     */
    public OutputStream append(final String filename) {
        try {
            if (!exists(filename)) {
                fs.create(new Path(filename)).close();
            }
            return fs.append(new Path(filename));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Creates an object container, e.g. a directory or a bucket (optional operation).
     *
     * @param filename the path of the container
     * @return true if the container does not exist and was successfully created; false otherwise.
     */

    public boolean create(final String filename) {
        try {
            return fs.mkdirs(new Path(filename));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Creates an object container, e.g. a directory or a bucket (optional operation).
     *
     * @param filename the path of the container
     * @param overwrite  overwrite
     * @return true if the container does not exist and was successfully created; false otherwise.
     */
    public OutputStream create(final String filename, boolean overwrite) {
        try {
            return fs.create(new Path(filename), overwrite);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Returns whether an object exists.
     *
     * <p>In stores supporting weak consistency models (e.g. eventual consistency) this operation
     * might affect semantics.
     *
     * @param filename the path to the object.
     * @return true if object exists, false otherwise.
     * @throws org.apache.kafka.connect.errors.DataException if the call to the underlying
     *         distributed storage failed.
     */
    public boolean exists(final String filename) {
        try {
            return fs.exists(new Path(filename));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Commit temp file, rename temp file.
     * @param tempFile temp file
     * @param committedFile  commit file
     */
    public void commit(final String tempFile, final String committedFile) {
        renameFile(tempFile, committedFile);
    }

    /**
     * Delete the given object or container.
     *
     * <p>In stores supporting weak consistency models (e.g. eventual consistency) the result of this
     * operation might not be immediately visible.
     *
     * @param filename the path to the object or container to delete.
     */
    public void delete(final String filename) {
        try {
            fs.delete(new Path(filename), true);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Stop using this storage.
     */
    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
    }

    /**
     * Get WAL .
     * @param storeConfig storeConfig
     * @param topicPart  kafka topic partition
     * @return WAL
     */
    public WAL wal(final StoreConfig storeConfig, final TopicPartition topicPart) {
        return new FSWAL(storeConfig, topicPart, this);
    }

    /**
     * Get the storage configuration.
     *
     * @return the storage configuration.
     */
    public HdfsSinkConfig conf() {
        return conf;
    }

    /**
     * Get the storage endpoint.
     *
     * @return the storage endpoint as a string.
     */
    public String url() {
        return url;
    }

    private void renameFile(final String sourcePath, final String targetPath) {
        if (sourcePath.equals(targetPath)) {
            return;
        }
        try {
            final Path srcPath = new Path(sourcePath);
            final Path dstPath = new Path(targetPath);
            if (fs.exists(srcPath)) {
                fs.rename(srcPath, dstPath);
            }
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Open for reading an object at the given path.
     *
     * @param filename the path of the object to be read.
     * @param conf storage configuration.
     * @return a seek-able input stream associated with the requested object.
     */
    public SeekableInput open(final String filename, final HdfsSinkConfig conf) {
        try {
            return new FsInput(new Path(filename), conf.getHadoopConfiguration());
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Get HDFS FileSystem.
     * @return FileSystem
     * */
    public FileSystem getFs() {
        return this.fs;
    }

    /**
     * Create a new file by given filename.
     * @param filename  filename
     */
    public void createFile(final String filename) {
        try {
            if (!exists(filename)) {
                fs.create(new Path(filename)).close();
            }
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Get file status by given filename.
     * @param fileName  filename
     * @return FileStatus
     */
    public FileStatus getFileStatus(final String fileName) {
        try {
            if (!exists(fileName)) {
                return null;
            }
            return fs.getFileStatus(new Path(fileName));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
