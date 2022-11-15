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

package cn.xdf.acdc.connect.hdfs.filter;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

import java.util.regex.Matcher;

public class TableTpCommittedFileFilter extends CommittedFileFilter {

    private TopicPartition tp;

    private String tableName;

    public TableTpCommittedFileFilter(final TopicPartition tp, final String tableName) {
        this.tp = tp;
        this.tableName = tableName;
    }

    @Override
    public boolean accept(final Path path) {
        if (!super.accept(path)) {
            return false;
        }
        String filename = path.getName();
        Matcher m = HdfsSinkConstants.COMMITTED_FILENAME_PATTERN.matcher(filename);
        // NB: if statement has side effect of enabling group() call
        if (!m.matches()) {
            throw new AssertionError("match expected because of CommittedFileFilter");
        }
        String tableNameReg = m.group(HdfsSinkConstants.PATTERN_TABLE_GROUP);
        int partition = Integer.parseInt(m.group(HdfsSinkConstants.PATTERN_PARTITION_GROUP));
        return tableNameReg.equals(this.tableName) && partition == tp.partition();
    }
}
