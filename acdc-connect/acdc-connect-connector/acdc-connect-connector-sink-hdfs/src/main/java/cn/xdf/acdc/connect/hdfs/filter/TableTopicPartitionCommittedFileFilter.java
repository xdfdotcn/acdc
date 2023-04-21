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

public class TableTopicPartitionCommittedFileFilter extends CommittedFileFilter {

    private int partition;

    private String topic;

    public TableTopicPartitionCommittedFileFilter(final TopicPartition topicPartition) {
        this.partition = topicPartition.partition();
        this.topic = topicPartition.topic();
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
        String matchedTopic = m.group(HdfsSinkConstants.PATTERN_TABLE_GROUP);
        int matchedPartition = Integer.parseInt(m.group(HdfsSinkConstants.PATTERN_PARTITION_GROUP));
        return matchedTopic.equals(this.topic) && matchedPartition == this.partition;
    }
}
