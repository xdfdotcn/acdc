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

package cn.xdf.acdc.connect.hdfs.storage.schema;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

public interface SchemaCompatibility {

    /**
     * Should be change schema.
     * @param record record
     * @param currentkeySchema key schema
     * @param currentValueSchema value schema
     * @return boolean
     */
    boolean shouldChangeSchema(
        ConnectRecord<?> record,
        Schema currentkeySchema,
        Schema currentValueSchema
    );

    /**
     * Project record.
     * @param record record
     * @param currentKeySchema key schema
     * @param currentValueSchema value schema
     * @return projected record
     */
    SinkRecord project(SinkRecord record, Schema currentKeySchema, Schema currentValueSchema);

    /**
     * Project record.
     * @param record record
     * @param currentKeySchema key schema
     * @param currentValueSchema value schema
     * @return projected record
     */
    SourceRecord project(SourceRecord record, Schema currentKeySchema, Schema currentValueSchema);
}
