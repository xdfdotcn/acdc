package cn.xdf.acdc.devops.service.dto;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class FieldMappingDTOTest {

    @Test
    public void testFindLogicalDelColumn() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__logical_del").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("yn").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .build()
        );

        Assertions.assertThat(FieldMappingDTO.findLogicalDelColumn(fieldMappings).isPresent()).isEqualTo(true);

        fieldMappings.clear();
        Assertions.assertThat(FieldMappingDTO.findLogicalDelColumn(fieldMappings).isPresent()).isEqualTo(false);

        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("logical_del").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("yn").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .build()
        );
        Assertions.assertThat(FieldMappingDTO.findLogicalDelColumn(fieldMappings).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindRowFilterExpress() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("sname").dataType("bigint(20)").allowNull("NO").build())
                .sinkField(FieldDTO.builder().name("name").dataType("bigint(20)").allowNull("NO").build())
                .filterOperator("  !=")
                .filterValue("y   ")
                .build()
        );
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("sage").dataType("bigint(20)").allowNull("NO").build())
                .sinkField(FieldDTO.builder().name("age").dataType("bigint(20)").allowNull("NO").build())
                .filterOperator("!=")
                .filterValue(" 15 ")
                .build()
        );
        Optional<String> opt = FieldMappingDTO.findRowFilterExpress(fieldMappings);
        Assertions.assertThat(opt.isPresent()).isEqualTo(true);
        Assertions.assertThat(opt.get()).isEqualTo("sname != y and sage != 15");

        fieldMappings.remove(1);
        opt = FieldMappingDTO.findRowFilterExpress(fieldMappings);
        Assertions.assertThat(opt.isPresent()).isEqualTo(true);
        Assertions.assertThat(opt.get()).isEqualTo("sname != y");

        fieldMappings.clear();
        opt = FieldMappingDTO.findRowFilterExpress(fieldMappings);
        Assertions.assertThat(opt.isPresent()).isEqualTo(false);

        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("age").dataType("bigint(20)").allowNull("NO").build())
                .sinkField(FieldDTO.builder().name("age").dataType("bigint(20)").allowNull("NO").build())
                .build()
        );
        opt = FieldMappingDTO.findRowFilterExpress(fieldMappings);
        Assertions.assertThat(opt.isPresent()).isEqualTo(false);
    }

    @Test
    public void testToSinkColumnMappingList() {
        List<SinkConnectorColumnMappingDO> expectSinkConnectorColumnMappings = Lists.newArrayList(
            SinkConnectorColumnMappingDO.builder().sourceColumnName("id\tbigint(20)\tPRI").sinkColumnName("id\tbigint(20)\tPRI").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("rdb_id\tbigint(20)\tPRI").sinkColumnName("rdb_id\tbigint(30)\tPRI").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("prj_id\tvarchar(15)\tMUL").sinkColumnName("prj_id\tvarchar(32)\tMUL").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__logical_del\tstring").sinkColumnName("yn\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__datetime\tstring").sinkColumnName("ods_update_time\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__op\tstring").sinkColumnName("opt\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__kafka_record_offset\tstring").sinkColumnName("version\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__none\tstring").sinkColumnName("my_field\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("name\tvarchar(15)").sinkColumnName("name\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("age\tbigint(20)").sinkColumnName("age\tbigint(20)").build()
        );

        List<SinkConnectorColumnMappingDO> actualSinkConnectorColumnMappings = FieldMappingDTO.toSinkColumnMappingList(createFieldMappings());
        actualSinkConnectorColumnMappings.forEach(m -> {
            m.setCreationTime(null);
            m.setUpdateTime(null);
        });
        Assertions.assertThat(expectSinkConnectorColumnMappings.toString()).isEqualTo(actualSinkConnectorColumnMappings.toString());

        actualSinkConnectorColumnMappings = FieldMappingDTO.toSinkColumnMappingList(Lists.newArrayList());
        Assertions.assertThat(actualSinkConnectorColumnMappings.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testToConnectorDataExtensionList() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("__datetime").dataType("string").allowNull("NO")
                    .build()
                )
                .sinkField(
                    FieldDTO.builder().name("ods_update_time").dataType("varchar(20)").allowNull("NO")
                        .build()
                )
                .build()
        );

        List<ConnectorDataExtensionDO> deList = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        Assertions.assertThat(deList.size()).isEqualTo(1);
        Assertions.assertThat(deList.get(0).getName()).isEqualTo("ods_update_time");
        Assertions.assertThat(deList.get(0).getValue()).isEqualTo("${datetime}");

        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("__datetime").dataType("string").allowNull("NO")
                    .build()
                )
                .sinkField(
                    FieldDTO.builder().name("etl_date").dataType("varchar(20)").allowNull("NO")
                        .build()
                )
                .build()
        );

        deList = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        Assertions.assertThat(deList.size()).isEqualTo(2);
        Assertions.assertThat(deList.get(0).getName()).isEqualTo("ods_update_time");
        Assertions.assertThat(deList.get(0).getValue()).isEqualTo("${datetime}");
        Assertions.assertThat(deList.get(1).getName()).isEqualTo("etl_date");
        Assertions.assertThat(deList.get(1).getValue()).isEqualTo("${datetime}");

        fieldMappings.clear();
        deList = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);

        Assertions.assertThat(deList.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testGetSourcePrimaryFields() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        // 主键 0
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder()
                    .name("id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
                )
                .build()
        );
        // 主键 1
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder()
                    .name("rdb_id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.UNI.name())
                    .build()
                )
                .build()
        );
        List<FieldDTO> fields = FieldMappingDTO.getSourcePrimaryFields(fieldMappings);
        Assertions.assertThat(fields.size()).isEqualTo(1);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("id");

        fieldMappings.remove(0);
        fields = FieldMappingDTO.getSourcePrimaryFields(fieldMappings);
        Assertions.assertThat(fields.size()).isEqualTo(1);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("rdb_id");
    }

    @Test
    public void testGetSinkPrimaryFields() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        // 主键 0
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sinkField(FieldDTO.builder()
                    .name("id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
                )
                .build()
        );
        // 主键 1
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sinkField(FieldDTO.builder()
                    .name("rdb_id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
                )
                .build()
        );
        List<FieldDTO> fields = FieldMappingDTO.getSinkPrimaryFields(fieldMappings);
        Assertions.assertThat(fields.size()).isEqualTo(2);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("id");
        Assertions.assertThat(fields.get(1).getName()).isEqualTo("rdb_id");

        fieldMappings.remove(0);
        fields = FieldMappingDTO.getSinkPrimaryFields(fieldMappings);
        Assertions.assertThat(fields.size()).isEqualTo(1);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("rdb_id");
    }

    @Test
    public void testFormatToField() {
        FieldDTO field = FieldMappingDTO.formatToField("name\tvarchar(20)");
        Assertions.assertThat(field.getName()).isEqualTo("name");
        Assertions.assertThat(field.getDataType()).isEqualTo("varchar(20)");
        Assertions.assertThat(field.getKeyType()).isEqualTo("");

        field = FieldMappingDTO.formatToField("age\tint\tPRI");

        Assertions.assertThat(field.getName()).isEqualTo("age");
        Assertions.assertThat(field.getDataType()).isEqualTo("int");
        Assertions.assertThat(field.getKeyType()).isEqualTo("PRI");
    }

    @Test
    public void testFormatToString() {
        FieldDTO dto = FieldDTO.builder()
            .name("id")
            .dataType("bit")
            .keyType("PRI")
            .allowNull("NO")
            .defaultValue("1")
            .build();

        String format = FieldMappingDTO.formatToString(dto);
        Assertions.assertThat(format).isEqualTo("id\tbit\tPRI");
    }

    @Test
    public void testMetaFormatToString() {
        String format = FieldMappingDTO.metaFormatToString("__logical_del");
        Assertions.assertThat(format).isEqualTo("__logical_del\tstring");
    }

    @Test
    public void testToFieldMapping() {
        ConnectionColumnConfigurationDO conf = ConnectionColumnConfigurationDO.builder()
            .filterOperator("!=")
            .filterValue("10")
            .sourceColumnName("id\tbit\tPRI")
            .sinkColumnName("tid\tbit\tPRI")
            .build();

        FieldMappingDTO expect = FieldMappingDTO.builder()
            .sourceField(FieldDTO.builder().name("id").dataType("bit").keyType("PRI").build())
            .sinkField(FieldDTO.builder().name("tid").dataType("bit").keyType("PRI").build())
            .filterOperator("!=")
            .filterValue("10")
            .matchStatus(FieldMappingDTO.IS_MATCH)
            .build();
        FieldMappingDTO fieldMapping = FieldMappingDTO.toFieldMapping(conf);
        Assertions.assertThat(expect).isEqualTo(fieldMapping);
    }

    @Test
    public void testToConnectionColumnConfiguration() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("id").dataType("bigint(20)").keyType("PRI").build())
                .sinkField(FieldDTO.builder().name("tid").dataType("bigint(20)").keyType("PRI").build())
                .filterOperator("!=")
                .filterValue("10")
                .matchStatus(FieldMappingDTO.IS_MATCH)
                .build(),
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder().name("sid").dataType("bigint(20)").keyType("PRI").build())
                .sinkField(FieldDTO.builder().name("oid").dataType("bigint(20)").keyType("PRI").build())
                .filterOperator(">")
                .filterValue("200")
                .matchStatus(FieldMappingDTO.IS_MATCH)
                .build()
        );

        List<ConnectionColumnConfigurationDO> configurations = FieldMappingDTO.toConnectionColumnConfiguration(fieldMappings);

        ConnectionColumnConfigurationDO first = configurations.get(0);
        ConnectionColumnConfigurationDO second = configurations.get(1);

        Assertions.assertThat(first.getSourceColumnName()).isEqualTo("id\tbigint(20)\tPRI");
        Assertions.assertThat(first.getSinkColumnName()).isEqualTo("tid\tbigint(20)\tPRI");
        Assertions.assertThat(first.getFilterOperator()).isEqualTo("!=");
        Assertions.assertThat(first.getFilterValue()).isEqualTo("10");

        Assertions.assertThat(second.getSourceColumnName()).isEqualTo("sid\tbigint(20)\tPRI");
        Assertions.assertThat(second.getSinkColumnName()).isEqualTo("oid\tbigint(20)\tPRI");
        Assertions.assertThat(second.getFilterOperator()).isEqualTo(">");
        Assertions.assertThat(second.getFilterValue()).isEqualTo("200");
    }

    /**
     * 字段映射集合, 包含是所有类型字段.
     * @return List
     */
    public static List<FieldMappingDTO> createFieldMappings() {
        List<FieldMappingDTO> fieldMappings = Lists.newArrayList();
        // 主键 0
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder()
                    .name("id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
                )
                .sinkField(
                    FieldDTO.builder().name("id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                        .build()
                )
                .build()
        );

        // 主键 1
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder()
                    .name("rdb_id").dataType("bigint(20)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
                )
                .sinkField(
                    FieldDTO.builder().name("rdb_id").dataType("bigint(30)").allowNull("NO").keyType(FieldKeyType.PRI.name())
                        .build()
                )
                .build()
        );

        // 其他类型键 2
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(FieldDTO.builder()
                    .name("prj_id").dataType("varchar(15)").allowNull("NO").keyType(FieldKeyType.MUL.name())
                    .build()
                )
                .sinkField(FieldDTO.builder()
                    .name("prj_id").dataType("varchar(32)").allowNull("NO").keyType(FieldKeyType.MUL.name())
                    .build()
                )
                .build()
        );

        // 逻辑删除字段 3
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__logical_del").dataType("string").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("yn").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .build()
        );

        // 写入时间戳 4
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__datetime").dataType("string").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("ods_update_time").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .build()
        );

        // 操作类型 5
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__op").dataType("string").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("opt").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .build()
        );

        // version 6
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__kafka_record_offset").dataType("string").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("version").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .build()
        );

        // none 7
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("__none").dataType("string").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("my_field").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .build()
        );

        // 普通字段 8
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("name").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("name").dataType("varchar(15)").allowNull("NO")
                        .build()
                )
                .filterOperator("!=")
                .filterValue("y")
                .build()
        );
        // 普通字段 9
        fieldMappings.add(
            FieldMappingDTO.builder()
                .sourceField(
                    FieldDTO.builder().name("age").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .sinkField(
                    FieldDTO.builder().name("age").dataType("bigint(20)").allowNull("NO")
                        .build()
                )
                .filterOperator("!=")
                .filterValue("15")
                .build()
        );

        return fieldMappings;
    }

    /**
     * Mock FieldMappingDTO.
     * @return FieldMappingDTO
     */
    public static FieldMappingDTO createFieldMapping() {
        return FieldMappingDTO.builder()
            .sourceField(
                FieldDTO.builder().name("source_id").dataType("int").allowNull("NO").keyType(FieldKeyType.UNI.name())
                    .build()
            )
            .sinkField(
                FieldDTO.builder().name("sink_id").dataType("bigint").allowNull("NO").keyType(FieldKeyType.PRI.name())
                    .build()
            )
            .filterOperator("!=")
            .filterValue("1")
            .build();
    }
}
