package cn.xdf.acdc.devops.core.domain.entity.enumeration;

// CHECKSTYLE:OFF

/**
 * kafka converter.
 */
public enum KafkaConverterType {

    CDC_V1("cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter"),

    JSON("org.apache.kafka.connect.json.JsonConverter"),

    SCHEMA_LESS_JSON("cn.xdf.acdc.connect.plugins.converter.json.SchemalessJsonConverter");

    final private String converterClass;

    KafkaConverterType(final String converterClass) {
        this.converterClass = converterClass;
    }

    public String getConverterClass() {
        return this.converterClass;
    }
}
