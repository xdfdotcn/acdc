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

package cn.xdf.acdc.connect.hdfs.wal;

import lombok.Getter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
public class WALEntry implements Writable {

    private String name;

    public WALEntry(final String name) {
        this.name = name;
    }

    public WALEntry() {
        name = null;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        name = Text.readString(in);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        Text.writeString(out, name);
    }
}
