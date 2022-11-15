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

package cn.xdf.acdc.connect.hdfs.initialize;

/**
 Hive integration mode.
 */
public enum StorageMode {

    EXACTLY_ONCE,

    AT_LEAST_ONCE;

    /**
     * Whether it is EXACTLY_ONCE.
     * @return boolean
     */
    public boolean isExactlyOnce() {
        return this == EXACTLY_ONCE;
    }

    /**
     * Whether it is AT_LEAST_ONCE.
     * @return boolean
     * */
    public boolean isAtLeastOnce() {
        return !isExactlyOnce();
    }
}
