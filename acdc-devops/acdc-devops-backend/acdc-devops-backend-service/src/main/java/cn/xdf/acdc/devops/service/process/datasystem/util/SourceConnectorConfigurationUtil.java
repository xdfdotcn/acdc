package cn.xdf.acdc.devops.service.process.datasystem.util;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

public class SourceConnectorConfigurationUtil {

    /**
     * Generate connector message key columns configuration for given.
     *
     * @param database database
     * @param tableDefinitions table definitions
     * @param pkIndexName primary key index name
     * @return message key
     */
    public static String generateConnectorMessageKeyColumnsConfigurationValue(
            final DataSystemResourceDTO database,
            final List<DataCollectionDefinition> tableDefinitions,
            final String pkIndexName) {
        StringBuilder result = new StringBuilder();

        tableDefinitions.forEach(each -> {
            Preconditions.checkArgument(!each.getUniqueIndexNameToFieldDefinitions().isEmpty(),
                    String.format("there must be a unique index in mysql table name: %s, database id: %d name %s",
                            each.getName(), database.getId(), database.getName()));

            List<DataFieldDefinition> messageKeyColumns = null;
            // if there is a PK in table, use it as message key columns.
            // else use fields in first unique index key
            if (each.getUniqueIndexNameToFieldDefinitions().containsKey(pkIndexName)) {
                messageKeyColumns = each.getUniqueIndexNameToFieldDefinitions().get(pkIndexName);
            } else {
                messageKeyColumns = each.getUniqueIndexNameToFieldDefinitions().values().stream().findFirst().get();
            }

            result.append(database.getName());
            result.append(CommonConstant.DOT);
            result.append(each.getName());
            result.append(CommonConstant.COLON);
            result.append(Joiner.on(CommonConstant.COMMA).join(messageKeyColumns.stream().map(eachColumn -> eachColumn.getName()).collect(Collectors.toList())));
            result.append(CommonConstant.SEMICOLON);
        });

        return result.toString();
    }
}
