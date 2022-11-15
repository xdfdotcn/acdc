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

@Getter
@Setter
public class TicdcEventBase {

    private TicdcEventType type;

    private int kafkaPartition;

    private long kafkaOffset;

    private long kafkaTimestamp;

    public TicdcEventBase() {
    }

    public TicdcEventBase(final TicdcEventType type, final KafkaMessage kafkaMessage) {
        this.type = type;
        this.kafkaPartition = kafkaMessage.getPartition();
        this.kafkaOffset = kafkaMessage.getOffset();
        this.kafkaTimestamp = kafkaMessage.getTimestamp();
    }
}
