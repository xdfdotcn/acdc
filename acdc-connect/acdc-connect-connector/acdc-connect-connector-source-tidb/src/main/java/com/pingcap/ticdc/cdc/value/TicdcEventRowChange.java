/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.ticdc.cdc.value;

import com.pingcap.ticdc.cdc.KafkaMessage;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class TicdcEventRowChange extends TicdcEventBase {
    // should be "u" or "d"
    private String updateOrDelete;

    private List<TicdcEventColumn> oldColumns;

    private List<TicdcEventColumn> columns;

    public TicdcEventRowChange(final KafkaMessage kafkaMessage) {
        super(TicdcEventType.rowChange, kafkaMessage);
    }

}
