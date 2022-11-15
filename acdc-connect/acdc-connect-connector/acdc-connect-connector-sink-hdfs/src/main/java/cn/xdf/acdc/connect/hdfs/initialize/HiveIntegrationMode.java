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

public enum HiveIntegrationMode {

    WITH_HIVE_META_DATA,

    AUTO_CREATE_EXTERNAL_TABLE,

    NONE;

    /**
     * Whether it is not NONE.
     * @return boolean
     */
    public boolean isIntegrationHive() {
        return this != NONE;
    }

    /**
     * Whether it is AUTO_CREATE_EXTERNAL_TABLE.
     * @return boolean
     */
    public boolean isAutoCreateTable() {
        return this == AUTO_CREATE_EXTERNAL_TABLE;
    }

    /**
     * Whether it is WITH_HIVE_META_DATA.
     * @return boolean
     */
    public boolean isWithHiveMetaData() {
        return this == WITH_HIVE_META_DATA;
    }
}
