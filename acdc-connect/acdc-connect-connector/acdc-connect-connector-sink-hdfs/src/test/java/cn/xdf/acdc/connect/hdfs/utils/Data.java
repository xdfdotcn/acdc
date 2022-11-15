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

package cn.xdf.acdc.connect.hdfs.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Data {

    private static final Map<String, List<Object>> DATA = new HashMap<>();

    /**
     * Get data.
     * @return map
     */
    public static Map<String, List<Object>> getData() {
        return DATA;
    }

    /**
     * Log contents.
     * @param message  message
     */
    public static void logContents(final String message) {
        if (log.isDebugEnabled()) {
            log.debug("{}: {}",
                message,
                DATA.entrySet()
                    .stream()
                    .map(e -> e.getKey()
                            + "="
                            + (
                            e.getValue() != null
                                ? (e.getValue()
                                .stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(",\n\t", "[\n\t", "]")))
                                : "null"
                        )
                    )
                    .collect(Collectors.joining(",\n")));
        }
    }
}
