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

import com.alibaba.fastjson.JSON;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StarRocksJsonSerializer implements StarRocksISerializer {
    
    public static final String NULL_STRING = "null";
    
    public static final String EMPTY_STRING = "";
    
    private final List<String> columns;
    
    public StarRocksJsonSerializer(final List<String> columns) {
        this.columns = columns;
    }
    
    @Override
    public byte[] serialize(final SinkRecord sinkRecord) {
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Object> rowMap = new LinkedHashMap<>(struct.schema().fields().size());
        
        for (String column : columns) {
            Object structValue = struct.get(column);
            if (Objects.isNull(structValue) || Objects.equals(structValue, NULL_STRING)) {
                rowMap.put(column, EMPTY_STRING);
            } else {
                rowMap.put(column, structValue);
            }
        }
        
        byte[] data = JSON.toJSONString(rowMap).getBytes(StandardCharsets.UTF_8);
        return data;
    }
    
}
