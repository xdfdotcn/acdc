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

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.jdbc.sink.JdbcSinkConfig;
import cn.xdf.acdc.connect.jdbc.util.ColumnDefinition;
import cn.xdf.acdc.connect.jdbc.util.ColumnId;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import lombok.Getter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@Getter
public abstract class BaseDialectTypeTest<T extends GenericDatabaseDialect> {

    public static final boolean NULLABLE = true;

    public static final boolean NOT_NULLABLE = false;

    public static final TableId TABLE_ID = new TableId(null, null, "MyTable");

    public static final ColumnId COLUMN_ID = new ColumnId(TABLE_ID, "columnA", "aliasA");

    public static final BigDecimal BIG_DECIMAL = new BigDecimal(9.9);

    public static final long LONG = Long.MAX_VALUE;

    public static final int INT = Integer.MAX_VALUE;

    public static final short SHORT = Short.MAX_VALUE;

    public static final byte BYTE = Byte.MAX_VALUE;

    public static final double DOUBLE = Double.MAX_VALUE;

    // todo: better way
    // CHECKSTYLE:OFF
    @Parameterized.Parameter(0)
    public Schema.Type expectedType;

    @Parameterized.Parameter(1)
    public Object expectedValue;

    @Parameterized.Parameter(2)
    public JdbcSinkConfig.NumericMapping numMapping;

    @Parameterized.Parameter(3)
    public boolean optional;

    @Parameterized.Parameter(4)
    public int columnType;

    @Parameterized.Parameter(5)
    public int precision;

    @Parameterized.Parameter(6)
    public int scale;

    protected boolean signed = true;

    protected T dialect;

    protected SchemaBuilder schemaBuilder;

    protected DatabaseDialect.ColumnConverter converter;
    // CHECKSTYLE:ON

    @Mock
    private ResultSet resultSet = mock(ResultSet.class);

    @Mock
    private ColumnDefinition columnDefn = mock(ColumnDefinition.class);

    @Before
    public void setup() throws Exception {
        dialect = createDialect();
    }

    @Test
    public void testValueConversion() throws Exception {
        // pass 测试的是JdbcSource的配置，故删除
    }

    /**
     * Create an instance of the dialect to be tested.
     *
     * @return the dialect; may not be null
     */
    protected abstract T createDialect();

    /**
     * Create a {@link JdbcSinkConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected JdbcSinkConfig sourceConfigWithUrl(
            final String url,
            final String... propertyPairs
    ) {
        Map<String, String> connProps = new HashMap<>();
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcSinkConfig.CONNECTION_URL, url);
        connProps.put(JdbcSinkConfig.NUMERIC_MAPPING_CONFIG, numMapping.toString());
        connProps.put(SinkConfig.DESTINATIONS, BaseDialectTest.DEFAULT_TEST_TABLE_NAME);
        connProps.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + BaseDialectTest.DEFAULT_TEST_TABLE_NAME + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, BaseDialectTest.DEFAULT_TEST_FIELD_NAME);
        return new JdbcSinkConfig(connProps);
    }

    protected Map<String, String> propertiesFromPairs(final String... pairs) {
        Map<String, String> props = new HashMap<>();
        assertEquals("Expecting even number of properties but found " + pairs.length, 0, pairs.length % 2);
        for (int i = 0; i != pairs.length; i += 2) {
            String key = pairs[i];
            String value = pairs[i + 1];
            props.put(key, value);
        }
        return props;
    }

}
