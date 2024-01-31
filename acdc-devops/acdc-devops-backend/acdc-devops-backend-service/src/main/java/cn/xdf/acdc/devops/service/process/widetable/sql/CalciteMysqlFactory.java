package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import com.google.common.collect.Lists;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CalciteMysqlFactory {
    
    private final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    
    private final CalciteConnectionConfig config;
    
    public CalciteMysqlFactory() {
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL.toString());
        properties.setProperty(CalciteConnectionProperty.FUN.camelName(), Lex.MYSQL.toString().toLowerCase());
        config = new CalciteConnectionConfigImpl(properties);
    }
    
    /**
     * Create a sql validator.
     *
     * @param schemaInfo schema info
     * @return sql validator
     */
    public SqlValidator createValidator(final Map<String, Map<String, List<DataFieldDefinition>>> schemaInfo) {
        SqlOperatorTable sqlOperatorTable = SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(EnumSet.of(SqlLibrary.MYSQL, SqlLibrary.STANDARD));
        return SqlValidatorUtil.newValidator(
                sqlOperatorTable,
                getCatalogReader(schemaInfo),
                factory,
                getValidatorConfig());
    }
    
    /**
     * Create a catalog reader.
     *
     * @param schemaInfo schema info
     * @return catalog reader
     */
    public Prepare.CatalogReader createCatalogReader(final Map<String, Map<String, List<DataFieldDefinition>>> schemaInfo) {
        return getCatalogReader(schemaInfo);
    }
    
    private SqlValidator.Config getValidatorConfig() {
        return SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
    }
    
    private Prepare.CatalogReader getCatalogReader(final Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        attachSchemasToRoot(schemaTableFieldsMap, rootSchema);
        
        String defaultSchema = schemaTableFieldsMap.keySet().stream().findFirst().orElseThrow(
                () -> new ServerErrorException("Schema table fields map is empty.")
        );
        return new CalciteCatalogReader(
                rootSchema,
                Lists.newArrayList(defaultSchema),
                factory,
                config);
    }
    
    private void attachSchemasToRoot(final Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap, final CalciteSchema rootSchema) {
        schemaTableFieldsMap.forEach((schemaName, tableFields) -> {
            RelationalBaseSchema.Builder schemaBuilder = RelationalBaseSchema.newBuilder(schemaName);
            tableFields.forEach((tableName, fields) -> {
                RelationalBaseTable.Builder tableBuilder = RelationalBaseTable.newBuilder(tableName);
                fields.forEach(field -> tableBuilder.addField(field.getName(), ConnectSchemaToCalciteSqlType.getCalciteSqlType(field.getConnectType())));
                RelationalBaseTable table = tableBuilder.build();
                schemaBuilder.addTable(table);
            });
            RelationalBaseSchema schema = schemaBuilder.build();
            rootSchema.add(schema.getSchemaName(), schema);
        });
    }
}
