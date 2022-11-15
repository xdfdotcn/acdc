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

package cn.xdf.acdc.connect.hdfs.hive.errors;

import org.apache.kafka.connect.errors.ConnectException;

public class HiveMetaStoreException extends ConnectException {

    private static final long serialVersionUID = 1L;

    public HiveMetaStoreException(final String s) {
        super(s);
    }

    public HiveMetaStoreException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public HiveMetaStoreException(final Throwable throwable) {
        super(throwable);
    }
}
