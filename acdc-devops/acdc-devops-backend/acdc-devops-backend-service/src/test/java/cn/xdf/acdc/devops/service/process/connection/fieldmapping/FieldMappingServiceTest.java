package cn.xdf.acdc.devops.service.process.connection.fieldmapping;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl.Jdbc2HiveFieldMappingServiceImpl;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class FieldMappingServiceTest {

    private FieldMappingService fieldMappingService;

    @Before
    public void setup() throws SQLException {
        fieldMappingService = new Jdbc2HiveFieldMappingServiceImpl();
    }

    @Test
    public void testDiffingField() {
        List<FieldMappingDTO> fieldMappings = fieldMappingService.diffingField(createSrcFieldMap(), createSinkFieldMap());
        Assertions.assertThat(fieldMappings.size()).isEqualTo(6);

        // 匹配到的字段,主键排序在第一位
        Assertions.assertThat(fieldMappings.get(0).getMatchStatus()).isEqualTo(FieldMappingDTO.IS_MATCH);
        Assertions.assertThat(fieldMappings.get(0).getSinkField().getKeyType()).isEqualTo(FieldKeyType.PRI.name());
        Assertions.assertThat(fieldMappings.get(0).getSinkField().getName()).isEqualTo(fieldMappings.get(0).getSourceField().getName());
        Assertions.assertThat(fieldMappings.get(0).getSinkField().getName()).isEqualTo("id");

        // 未匹配到的字段,主键字段排在前面
        Assertions.assertThat(fieldMappings.get(3).getSinkField().getName()).isEqualTo("email2");
        Assertions.assertThat(fieldMappings.get(3).getMatchStatus()).isEqualTo(FieldMappingDTO.NOT_MATCH);

        // 三个字段匹配,三个字段不匹配
        List<FieldMappingDTO> matchFields = fieldMappings.stream().filter(fm -> fm.getMatchStatus() == FieldMappingDTO.IS_MATCH).collect(Collectors.toList());
        Set<String> matchFieldSet = Sets.newHashSet();
        matchFieldSet.add("id");
        matchFieldSet.add("name");
        matchFieldSet.add("creation_time");

        List<FieldMappingDTO> notMatchFields = fieldMappings.stream().filter(fm -> fm.getMatchStatus() == FieldMappingDTO.NOT_MATCH).collect(Collectors.toList());
        Set<String> notMatchFieldSet = Sets.newHashSet();
        notMatchFieldSet.add("ods_update_time");
        notMatchFieldSet.add("email2");
        notMatchFieldSet.add("yn");

        Assertions.assertThat(matchFields.size()).isEqualTo(3);
        Assertions.assertThat(notMatchFields.size()).isEqualTo(3);

        matchFields.forEach(fm -> {
            Assertions.assertThat(fm.getSinkField().getName()).isEqualTo(fm.getSourceField().getName());
            Assertions.assertThat(fm.getMatchStatus()).isEqualTo(FieldMappingDTO.IS_MATCH);
            Assertions.assertThat(matchFieldSet.contains(fm.getSinkField().getName())).isEqualTo(true);
        });

        notMatchFields.forEach(fm -> {
            Assertions.assertThat(fm.getSourceField().getName()).isEqualTo("");
            Assertions.assertThat(fm.getMatchStatus()).isEqualTo(FieldMappingDTO.NOT_MATCH);
            Assertions.assertThat(notMatchFieldSet.contains(fm.getSinkField().getName())).isEqualTo(true);
        });
    }

    private Map<String, FieldDTO> createSrcFieldMap() {
        Map<String, FieldDTO> fieldMap = Maps.newHashMap();
        fieldMap.put("id", FieldDTO.builder()
            .name("id")
            .dataType("bigint(20)")
            .allowNull("NO")
            .keyType("PRI")
            .defaultValue("")
            .extra("auto_increment")
            .build()
        );
        fieldMap.put("email", FieldDTO.builder()
            .name("email")
            .dataType("bigint(20)")
            .allowNull("NO")
            .keyType("UNI")
            .defaultValue("")
            .extra("auto_increment")
            .build()
        );

        fieldMap.put("name", FieldDTO.builder()
            .name("name")
            .dataType("varchar(128)")
            .allowNull("NO")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );

        fieldMap.put("creation_time", FieldDTO.builder()
            .name("creation_time")
            .dataType("datetime(3)")
            .allowNull("YES")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        fieldMap.put("update_time", FieldDTO.builder()
            .name("update_time")
            .dataType("datetime(3)")
            .allowNull("YES")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        return fieldMap;
    }

    private Map<String, FieldDTO> createSinkFieldMap() {
        Map<String, FieldDTO> fieldMap = Maps.newHashMap();

        fieldMap.put("id", FieldDTO.builder()
            .name("id")
            .dataType("bigint(20)")
            .allowNull("NO")
            .keyType("PRI")
            .defaultValue("")
            .extra("auto_increment")
            .build()
        );
        fieldMap.put("email2", FieldDTO.builder()
            .name("email2")
            .dataType("bigint(20)")
            .allowNull("NO")
            .keyType("PRI")
            .defaultValue("")
            .extra("auto_increment")
            .build()
        );

        fieldMap.put("name", FieldDTO.builder()
            .name("name")
            .dataType("varchar(128)")
            .allowNull("NO")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        fieldMap.put("creation_time", FieldDTO.builder()
            .name("creation_time")
            .dataType("datetime(3)")
            .allowNull("YES")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        fieldMap.put("ods_update_time", FieldDTO.builder()
            .name("ods_update_time")
            .dataType("datetime(3)")
            .allowNull("YES")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        fieldMap.put("yn", FieldDTO.builder()
            .name("yn")
            .dataType("varchar(1)")
            .allowNull("YES")
            .keyType("")
            .defaultValue("")
            .extra("")
            .build()
        );
        return fieldMap;
    }
}
