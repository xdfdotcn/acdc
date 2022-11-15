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

package cn.xdf.acdc.connect.hdfs.rotation;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;

public class FileSizeRotationPolicy implements RotationPolicy {

    private long rotationFileSize;

    public FileSizeRotationPolicy(
        final HdfsSinkConfig hdfsSinkConfig
    ) {
        this.rotationFileSize = hdfsSinkConfig.getLong(HdfsSinkConfig.ROTATION_POLICY_FILE_SIZE);
    }

    @Override
    public boolean shouldBeRotateFile(final RecordWriter recordWriter) {
        if (recordWriter.fileSize() >= rotationFileSize) {
            return true;
        }
        return false;
    }

    @Override
    public void reset() {

    }
}
