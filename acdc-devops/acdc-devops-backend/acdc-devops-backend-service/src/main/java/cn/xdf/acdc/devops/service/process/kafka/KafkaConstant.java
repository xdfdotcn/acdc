package cn.xdf.acdc.devops.service.process.kafka;

public class KafkaConstant {

    public static final String SECURITY_PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT";

    public static final String SASL_MECHANISM_PLAIN = "PLAIN";

    public static final String SASL_JAAS_CONFIG_PATTERN = "%s required username=\"%s\" password=\"%s\";";

    public static final String SCRAM_LOGIN_MODULE_CLASS = "org.apache.kafka.common.security.scram.ScramLoginModule";

    public static final String PLAIN_LOGIN_MODULE_CLASS = "org.apache.kafka.common.security.plain.PlainLoginModule";
}
