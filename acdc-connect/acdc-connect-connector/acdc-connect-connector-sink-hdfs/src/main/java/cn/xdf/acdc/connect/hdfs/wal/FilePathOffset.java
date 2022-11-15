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

import lombok.Getter;

/**
 * A class to keep track of an offset and filepath pair.
 *
 * <p>The offset may be extracted from the filepath in the same function. Extracting the offset from
 * the file path involves O(n) time complexity. This class can be used to link the extracted offset
 * to the filepath to avoid the re-computation of extracting the offset.</p>
 */
@Getter
public class FilePathOffset {

    private final long offset;

    private final String filePath;

    public FilePathOffset(long offset, final String filePath) {
        this.offset = offset;
        this.filePath = filePath;
    }

    @Override
    public String toString() {
        return "FilePathOffset{"
            + "offset=" + offset
            + ", filePath='" + filePath + '\''
            + '}';
    }
}
