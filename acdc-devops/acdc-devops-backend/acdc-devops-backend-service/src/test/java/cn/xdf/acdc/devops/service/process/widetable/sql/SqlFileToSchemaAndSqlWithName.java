package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.relational.RelationalDataSystemDataCollectionDefinitionUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class SqlFileToSchemaAndSqlWithName {
    
    private static final String SQL_FILE_ROOT_PATH = "/calcite/validate-and-optimize/sql/";
    
    /**
     * Get schemas and sqls.
     *
     * @param fileName file name
     * @return schemas and sqls
     */
    public static SchemasAndSqls getSchemasAndSqls(final String fileName) {
        Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap = new HashMap<>();
        Map<String, String> sqlMap = new HashMap<>();
        List<String> text = read(fileName);
        AtomicReference<String> currentContext = new AtomicReference<>();
        text.forEach(lineInText -> {
            String line = lineInText.trim();
            if (line.startsWith("-- table:") || line.startsWith("-- sql: ")) {
                currentContext.set(line);
                return;
            }
            if (currentContext.get().startsWith("-- table:")) {
                String context = currentContext.get();
                String dbTable = context.substring(context.indexOf(":") + 1);
                String db = dbTable.split("\\.")[0].trim();
                String tb = dbTable.split("\\.")[1].trim();
                line = line.replace("--", "");
                String[] fieldSplits = line.split("\\|");
                String fieldName = fieldSplits[0].trim();
                String fieldType = fieldSplits[1].trim();
                Set<String> fieldUkSet = fieldSplits.length > 2
                        ? Arrays.stream(fieldSplits[2].trim().split(",")).map(String::trim).collect(Collectors.toSet()) : new HashSet<>();
                schemaTableFieldsMap.computeIfAbsent(db, k -> new HashMap<>()).computeIfAbsent(tb, k -> new ArrayList<>())
                        .add(new DataFieldDefinition(fieldName,
                                fieldType,
                                RelationalDataSystemDataCollectionDefinitionUtil.getConnectSchema(fieldType),
                                true,
                                null,
                                RelationalDataSystemDataCollectionDefinitionUtil.getFieldExtendProperties(fieldType),
                                fieldUkSet));
            }
            if (currentContext.get().startsWith("-- sql:")) {
                String context = currentContext.get();
                String sqlName = context.substring(context.indexOf(":") + 1).trim();
                String sql = line.trim();
                sqlMap.put(sqlName, sqlMap.getOrDefault(sqlName, "") + "\n" + sql);
            }
        });
        return new SchemasAndSqls(schemaTableFieldsMap, sqlMap);
    }
    
    private static List<String> read(final String sqlFileName) {
        InputStream input = SqlFileToSchemaAndSqlWithName.class.getResourceAsStream(SQL_FILE_ROOT_PATH + sqlFileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(input)));
        List<String> result = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                result.add(line);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                reader.close();
                input.close();
            } catch (IOException e) {
                log.warn("IO close exception: {}", e.toString());
            }
        }
        return result;
    }
}
