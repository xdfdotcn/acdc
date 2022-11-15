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

import org.apache.kafka.common.config.ConfigException;
import org.apache.parquet.Strings;

public final class FilePath {

    private static final String PATH_SEPARATOR = "/";

    private static final String PATH_REPAIR_REG = "^/|/$";

    private static final String URL_PREFIX = "hdfs:";

    private final StringBuilder pathBuffer = new StringBuilder();

    private FilePath() {

    }

    /**
     * Get file path.
     * @return the file path
     */
    public String path() {
        return pathBuffer.toString();
    }

    /**
     * The specified base path,maybe with url.
     * @param basePath root path
     * @return builder
     */
    public static FilePathBuilder of(final String basePath) {
        return new FilePathBuilder(basePath);
    }

    public static class FilePathBuilder {

        private final FilePath filePath;

        public FilePathBuilder(final String basePath) {
            this.filePath = new FilePath();
            if (!isUrl(basePath)) {
                this.filePath.pathBuffer.append(PATH_SEPARATOR);
            }
            this.filePath.pathBuffer.append(basePath);
        }

        /**
         * Stitching path.
         * @param path  sub file path
         * @return builder
         */
        public FilePathBuilder join(final String path) {
            this.filePath.pathBuffer
                .append(PATH_SEPARATOR)
                .append(repairPath(path));
            return this;
        }

        /**
         * Build path.
         * @return FilePath
         */
        public FilePath build() {
            if (this.filePath.pathBuffer.length() <= 0) {
                throw new ConfigException("Path is empty");
            }
            return this.filePath;
        }

        /**
         Repair path start or end  with "/",replace empty.
         */
        private String repairPath(final String path) {
            if (Strings.isNullOrEmpty(path)) {
                throw new ConfigException("Invalided path: " + path);
            }
            return path.replaceAll(PATH_REPAIR_REG, "");
        }

        private boolean isUrl(final String path) {
            return !Strings.isNullOrEmpty(path) && path.startsWith(URL_PREFIX);
        }
    }
}
