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

package cn.xdf.acdc.connect.jdbc.dialect;

import cn.xdf.acdc.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class GenericDatabaseDialectTypeTest extends BaseDialectTypeTest<GenericDatabaseDialect> {

    @Parameterized.Parameters
    public static Iterable<Object[]> mapping() {
        return Arrays.asList(
                new Object[][] {
                        // MAX_VALUE means this value doesn't matter
                        // Parameter range 1-4
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.NONE, NOT_NULLABLE,
                                Types.NUMERIC, Integer.MAX_VALUE, 0},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.NONE, NULLABLE,
                                Types.NUMERIC, Integer.MAX_VALUE, -127},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.NONE, NULLABLE,
                                Types.NUMERIC, Integer.MAX_VALUE, 0},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.NONE, NULLABLE,
                                Types.NUMERIC, Integer.MAX_VALUE, -127},

                        // integers - non optional
                        // Parameter range 5-8
                        {Schema.Type.INT64, LONG, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE,
                                Types.NUMERIC, 18, 0},
                        {Schema.Type.INT32, INT, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE,
                                Types.NUMERIC, 8, 0},
                        {Schema.Type.INT16, SHORT, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NOT_NULLABLE, Types.NUMERIC, 3, 0},
                        {Schema.Type.INT8, BYTE, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE,
                                Types.NUMERIC, 1, 0},

                        // integers - optional
                        // Parameter range 9-12
                        {Schema.Type.INT64, LONG, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NULLABLE,
                                Types.NUMERIC, 18, 0},
                        {Schema.Type.INT32, INT, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NULLABLE,
                                Types.NUMERIC, 8, 0},
                        {Schema.Type.INT16, SHORT, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NULLABLE,
                                Types.NUMERIC, 3, 0},
                        {Schema.Type.INT8, BYTE, JdbcSinkConfig.NumericMapping.PRECISION_ONLY, NULLABLE,
                                Types.NUMERIC, 1, 0},

                        // scale != 0 - non optional
                        // Parameter range 13-16
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NOT_NULLABLE, Types.NUMERIC, 18, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NOT_NULLABLE, Types.NUMERIC, 8, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NOT_NULLABLE, Types.NUMERIC, 3, -1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NOT_NULLABLE, Types.NUMERIC, 1, -1},

                        // scale != 0 - optional
                        // Parameter range 17-20
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NULLABLE, Types.NUMERIC, 18, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NULLABLE, Types.NUMERIC, 8, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NULLABLE, Types.NUMERIC, 3, -1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.PRECISION_ONLY,
                                NULLABLE, Types.NUMERIC, 1, -1},

                        // integers - non optional
                        // Parameter range 21-25
                        {Schema.Type.INT64, LONG, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 18, -1},
                        {Schema.Type.INT32, INT, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 8, -1},
                        {Schema.Type.INT16, SHORT, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 3, 0},
                        {Schema.Type.INT8, BYTE, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 1, 0},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.BEST_FIT,
                                NOT_NULLABLE, Types.NUMERIC, 19, -1},

                        // integers - optional
                        // Parameter range 26-30
                        {Schema.Type.INT64, LONG, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 18, -1},
                        {Schema.Type.INT32, INT, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 8, -1},
                        {Schema.Type.INT16, SHORT, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 3, 0},
                        {Schema.Type.INT8, BYTE, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 1, 0},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 19, -1},

                        // floating point - fitting - non optional
                        {Schema.Type.FLOAT64, DOUBLE, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 18, 127},
                        {Schema.Type.FLOAT64, DOUBLE, JdbcSinkConfig.NumericMapping.BEST_FIT, NOT_NULLABLE,
                                Types.NUMERIC, 8, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.BEST_FIT,
                                NOT_NULLABLE, Types.NUMERIC, 19, 1},

                        // floating point - fitting - optional
                        {Schema.Type.FLOAT64, DOUBLE, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 18, 127},
                        {Schema.Type.FLOAT64, DOUBLE, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 8, 1},
                        {Schema.Type.BYTES, BIG_DECIMAL, JdbcSinkConfig.NumericMapping.BEST_FIT, NULLABLE,
                                Types.NUMERIC, 19, 1},

                }
        );
    }

    @Override
    protected GenericDatabaseDialect createDialect() {
        return new GenericDatabaseDialect(sourceConfigWithUrl("jdbc:some:db"));
    }

}
