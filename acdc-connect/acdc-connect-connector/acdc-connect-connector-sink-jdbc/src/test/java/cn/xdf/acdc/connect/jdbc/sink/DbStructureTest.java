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

package cn.xdf.acdc.connect.jdbc.sink;

import cn.xdf.acdc.connect.core.sink.metadata.FieldsMetadata;
import cn.xdf.acdc.connect.core.sink.metadata.SinkRecordField;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.TableDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import cn.xdf.acdc.connect.jdbc.util.TableType;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.mockito.Matchers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DbStructureTest {

    private DatabaseDialect dbDialect = mock(DatabaseDialect.class);

    private DbStructure structure = new DbStructure(dbDialect);

    private Connection connection = mock(Connection.class);

    private TableId tableId = mock(TableId.class);

    private JdbcSinkConfig config = mock(JdbcSinkConfig.class);

    private FieldsMetadata fieldsMetadata = new FieldsMetadata(new HashSet<>(), new HashSet<>(), new HashMap<>());

    static Set<String> columns(final String... names) {
        return new HashSet<>(Arrays.asList(names));
    }

    static List<SinkRecordField> sinkRecords(final String... names) {
        List<SinkRecordField> fields = new ArrayList<>();
        for (String n : names) {
            fields.add(field(n));
        }
        return fields;
    }

    static SinkRecordField field(final String name) {
        return new SinkRecordField(Schema.STRING_SCHEMA, name, false);
    }

    @Test
    public void testNoMissingFields() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aaa", "bbb")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithSameCase() {
        assertEquals(1, missingFields(sinkRecords("aaa", "bbb"), columns("aaa")).size());
    }

    @Test
    public void testSameNamesDifferentCases() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aAa", "AaA")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithDifferentCase() {
        assertTrue(missingFields(sinkRecords("aaa", "bbb"), columns("AaA", "BbB")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aaa", "bbb")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aAa", "BbB")).isEmpty());
    }

    @Test(expected = TableAlterOrCreateException.class)
    public void testMissingTableNoAutoCreate() throws Exception {
        structure.create(config, connection, tableId,
                fieldsMetadata);
    }

    @Test(expected = TableAlterOrCreateException.class)
    public void testAlterNoAutoEvolve() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(Matchers.any(), Matchers.any())).thenReturn(true);
        when(dbDialect.describeTable(Matchers.any(), Matchers.any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.TABLE);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                false
        );

        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.getName()),
                Collections.singletonMap(sinkRecordField.getName(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test(expected = TableAlterOrCreateException.class)
    public void testAlterNotSupported() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(Matchers.any(), Matchers.any())).thenReturn(true);
        when(dbDialect.describeTable(Matchers.any(), Matchers.any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                true
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.getName()),
                Collections.singletonMap(sinkRecordField.getName(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test(expected = TableAlterOrCreateException.class)
    public void testCannotAlterBecauseFieldNotOptionalAndNoDefaultValue() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(Matchers.any(), Matchers.any())).thenReturn(true);
        when(dbDialect.describeTable(Matchers.any(), Matchers.any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.INT32_SCHEMA,
                "test",
                true
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.getName()),
                Collections.singletonMap(sinkRecordField.getName(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test(expected = TableAlterOrCreateException.class)
    public void testFailedToAmendExhaustedRetry() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(Matchers.any(), Matchers.any())).thenReturn(true);
        when(dbDialect.describeTable(Matchers.any(), Matchers.any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                false
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.getName()),
                Collections.singletonMap(sinkRecordField.getName(), sinkRecordField));

        Map<String, String> props = new HashMap<>();

        // Required configurations, set to empty strings because they are irrelevant for the test
        props.put("connection.url", "");
        props.put("connection.user", "");
        props.put("connection.password", "");

        // Set to true so that the connector does not throw the exception on a different condition
        props.put("auto.evolve", "true");
        props.put("destinations", "table_1");
        props.put("destinations.table_1.fields.whitelist", "field_1");
        JdbcSinkConfig config = new JdbcSinkConfig(props);

        doThrow(new SQLException()).when(dbDialect).applyDdlStatements(Matchers.any(), Matchers.any());

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 0);
    }

    private Set<SinkRecordField> missingFields(
            final Collection<SinkRecordField> fields,
            final Set<String> dbColumnNames
    ) {
        return structure.missingFields(fields, dbColumnNames);
    }
}
