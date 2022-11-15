package cn.xdf.acdc.connect.kafka.sink;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class KafkaSinkConfig extends SinkConfig {

    public static final String SINK_KAFKA_PREFIX = "sink.kafka.";

    public static final String SINK_KAFKA_SERVERS = SINK_KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    public static final String SINK_KAFKA_SERVERS_DOC = "sink kafka servers.\n"
            + " For example: ``kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092``";

    public static final String SINK_KAFKA_SERVERS_DISPLAY = "SINK KAFKA SERVERS";

    public static final String SINK_KAFKA_KEY_CONVERTER = SINK_KAFKA_PREFIX + "key.converter";

    public static final String SINK_KAFKA_KEY_CONVERTER_DOC = "sink kafka key converter.";

    public static final String SINK_KAFKA_KEY_CONVERTER_DISPLAY = "SINK KAFKA KEY CONVERTER";

    public static final String SINK_KAFKA_VALUE_CONVERTER = SINK_KAFKA_PREFIX + "value.converter";

    public static final String SINK_KAFKA_VALUE_CONVERTER_DOC = "sink kafka value converter.";

    public static final String SINK_KAFKA_VALUE_CONVERTER_DISPLAY = "SINK KAFKA VALUE CONVERTER";

    public static final String SINK_KAFKA_CONVERTER_DEFAULT = "org.apache.kafka.connect.json.JsonConverter";

    public static final String SINK_KAFKA_GROUP = "Sink-kafka";

    static {
        CONFIG_DEF.define(
                SINK_KAFKA_SERVERS,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                SINK_KAFKA_SERVERS_DOC,
                SINK_KAFKA_GROUP,
                1,
                ConfigDef.Width.LONG,
                SINK_KAFKA_SERVERS_DISPLAY
        ).define(
                SINK_KAFKA_KEY_CONVERTER,
                ConfigDef.Type.STRING,
                SINK_KAFKA_CONVERTER_DEFAULT,
                ConfigDef.Importance.HIGH,
                SINK_KAFKA_KEY_CONVERTER_DOC,
                SINK_KAFKA_GROUP,
                2,
                ConfigDef.Width.LONG,
                SINK_KAFKA_KEY_CONVERTER_DISPLAY
        ).define(
                SINK_KAFKA_VALUE_CONVERTER,
                ConfigDef.Type.STRING,
                SINK_KAFKA_CONVERTER_DEFAULT,
                ConfigDef.Importance.HIGH,
                SINK_KAFKA_VALUE_CONVERTER_DOC,
                SINK_KAFKA_GROUP,
                3,
                ConfigDef.Width.LONG,
                SINK_KAFKA_VALUE_CONVERTER_DISPLAY
        );
    }

    public KafkaSinkConfig(final Map<String, String> props) {
        super(props);
    }

}
