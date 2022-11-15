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

package cn.xdf.acdc.connect.hdfs.initialize;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.Format;
import com.google.common.base.Strings;
import org.apache.kafka.common.config.ConfigException;

public class StoreConfig {

    public static final String TXT_SEPARATOR = "field.delim";

    private String storeRootPath;

    private String walLogPath;

    private String tmpTablePath;

    private String tablePath;

    private String table;

    private String database;

    private String storeUrl;

    private String txtSeparator;

    private Format format;

    /**
     * Storage root path .
     * @return the store root path
     */
    public String storeRootPath() {
        return this.storeRootPath;
    }

    /**
     * WAL log path .
     * @return WAL log path
     */
    public String walLogPath() {
        return this.walLogPath;
    }

    /**
     * Temp file path.
     * @return temp file path
     */
    public String tmpTablePath() {
        return this.tmpTablePath;
    }

    /**
     * Hive table's file store path.
     * @return temp file path
     */
    public String tablePath() {
        return this.tablePath;
    }

    /**
     * Table name.
     * @return temp file path
     */
    public String table() {
        return this.table;
    }

    /**
     * Database name .
     * @return database name
     */
    public String database() {
        return this.database;
    }

    /**
     * store url eg :hdfs://hostname:8020.
     * @return store url
     */
    public String storeUrl() {
        return this.storeUrl;
    }

    /**
     * Format is text, the separator.
     * @return separator
     */
    public String textSeparator() {
        return this.txtSeparator;
    }

    /**
     * Get format .
     * @return Format
     */
    public Format format() {
        return this.format;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StoreConfig{");
        sb.append("storeRootPath='").append(storeRootPath).append('\'');
        sb.append(", walLogPath='").append(walLogPath).append('\'');
        sb.append(", tmpTablePath='").append(tmpTablePath).append('\'');
        sb.append(", tablePath='").append(tablePath).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", database='").append(database).append('\'');
        sb.append(", storeUrl='").append(storeUrl).append('\'');
        sb.append(", txtSeparator='").append(txtSeparator).append('\'');
        sb.append(", format=").append(format);
        sb.append('}');
        return sb.toString();
    }

    // CHECKSTYLE:OFF
    public static StorageConfBuilder builder() {
        StoreConfig storeConfig = new StoreConfig();
        return new StorageConfBuilder(storeConfig);
    }
    // CHECKSTYLE:ON


    // CHECKSTYLE:OFF
    public static class StorageConfBuilder {

        private StoreConfig storeConfig;

        public StorageConfBuilder(final StoreConfig storeConfig) {
            this.storeConfig = storeConfig;
        }

        public StorageConfBuilder storeRootPath(final String storeRootPath) {
            notBlank(storeRootPath);
            storeConfig.storeRootPath = storeRootPath;
            return this;
        }

        public StorageConfBuilder walLogPath(final String walLogPath) {
            notBlank(walLogPath);
            storeConfig.walLogPath = walLogPath;
            return this;
        }

        public StorageConfBuilder tmpTablePath(final String tmpTablePath) {
            notBlank(tmpTablePath);
            storeConfig.tmpTablePath = tmpTablePath;
            return this;
        }

        public StorageConfBuilder tablePath(final String tablePath) {
            notBlank(tablePath);
            storeConfig.tablePath = tablePath;
            return this;
        }

        public StorageConfBuilder table(final String table) {
            notBlank(table);
            storeConfig.table = table;
            return this;
        }

        public StorageConfBuilder database(final String database) {
            notBlank(database);
            storeConfig.database = database;
            return this;
        }

        public StorageConfBuilder storeUrl(final String storeUrl) {
            notBlank(storeUrl);
            storeConfig.storeUrl = storeUrl;
            return this;
        }

        public StorageConfBuilder txtSeparator(final String txtSeparator) {
            if (Strings.isNullOrEmpty(txtSeparator)) {
                storeConfig.txtSeparator = HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR_DEFAULT;
            } else {
                storeConfig.txtSeparator = txtSeparator;
            }
            return this;
        }

        public StorageConfBuilder classNameOfFormat(final String className) {
            Format format = Format.classNameOf(className);
            storeConfig.format = format;
            return this;
        }

        public StorageConfBuilder format(final String format) {
            notBlank(format);
            storeConfig.format = Format.valueOf(format);
            return this;
        }

        public StoreConfig build() {
            return storeConfig;
        }

        private void notBlank(String config) {
            if (Strings.isNullOrEmpty(config)) {
                throw new ConfigException("Value must be set.");
            }
        }
    }
    // CHECKSTYLE:ON
}
