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

package cn.xdf.acdc.connect.hdfs.hive.schema;

import cn.xdf.acdc.connect.hdfs.common.SchemaGenerator;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DefaultSchemaGenerator implements SchemaGenerator<FieldSchema> {

    public DefaultSchemaGenerator() {

    }

    public DefaultSchemaGenerator(final Map<String, Object> config) {
        // no configs are used
    }

    @Override
    public List<FieldSchema> newPartitionFields(final String partitionFields) {
        String[] fields = partitionFields.split(",");
        List<FieldSchema> result = new ArrayList<>();

        for (String field : fields) {
            result.add(
                new FieldSchema(field, TypeInfoFactory.stringTypeInfo.toString(), "")
            );
        }

        return Collections.unmodifiableList(result);
    }
}
