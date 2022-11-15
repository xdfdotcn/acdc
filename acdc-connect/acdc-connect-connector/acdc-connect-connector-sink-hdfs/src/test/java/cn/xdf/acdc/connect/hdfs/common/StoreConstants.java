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

package cn.xdf.acdc.connect.hdfs.common;

public class StoreConstants {

    public static final String HIVE_DB = "default";

    public static final String HIVE_TABLE = "test_sink";

    public static final String HDFS_ROOT = "/unit_test";

    public static final String TABLES = HIVE_DB + "." + HIVE_TABLE;
}
