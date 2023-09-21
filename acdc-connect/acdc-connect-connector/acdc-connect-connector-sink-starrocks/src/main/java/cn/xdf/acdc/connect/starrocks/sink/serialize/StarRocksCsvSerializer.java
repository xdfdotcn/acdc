/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xdf.acdc.connect.starrocks.sink.serialize;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StarRocksCsvSerializer implements StarRocksISerializer {
    
    public static final String NULL_VALUE = "\\N";
    
    public static final String BLANK_STRING = " ";
    
    private final String columnSeparator;
    
    private final List<String> columns;
    
    public StarRocksCsvSerializer(final String columnSeparator, final List<String> columns) {
        this.columnSeparator = StarRocksDelimiterParser.parse(columnSeparator, columnSeparator);
        this.columns = columns;
    }
    
    @Override
    public byte[] serialize(final SinkRecord sinkRecord) {
        Struct struct = (Struct) sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();
        List<Object> data = new ArrayList<>();
        
        for (String column : columns) {
            Field sourceField = valueSchema.field(column);
            Object structValue;
            if (Objects.isNull(sourceField)
                    || (structValue = struct.get(column)) == null
            ) {
                data.add(NULL_VALUE);
                continue;
            }
            
            String structValueString = String.valueOf(structValue);
            String writeLine = CharMatcher.breakingWhitespace().replaceFrom(structValueString, BLANK_STRING);
            data.add(writeLine);
        }
        return Joiner.on(columnSeparator).join(data).getBytes(StandardCharsets.UTF_8);
    }
}
