package cn.xdf.acdc.connect.core.sink.filter;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.core.util.DateUtils;
import cn.xdf.acdc.connect.core.util.config.DestinationConfig;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import com.google.common.base.Strings;
import com.ql.util.express.DefaultContext;
import com.ql.util.express.ExpressRunner;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConditionsFilter implements Filter {
    
    private static final String EXPRESS_TEMPLATE = "return %s";
    
    private String express;
    
    private String condition;
    
    private ExpressRunner expressRunner = new ExpressRunner();
    
    private Set<Field> fieldSet = new HashSet<>();
    
    private String destination;
    
    private TimeZone timeZone;
    
    public ConditionsFilter(final DestinationConfig destinationConfig, final TimeZone timeZone) {
        this.destination = destinationConfig.getName();
        this.timeZone = timeZone;
        this.condition = destinationConfig.getRowFilterExpress();
        this.express = String.format(EXPRESS_TEMPLATE, condition);
    }
    
    @SneakyThrows
    @Override
    public boolean filter(final SinkRecord sinkRecord) {
        if (Strings.isNullOrEmpty(condition)) {
            return true;
        }
        if (fieldSet.isEmpty()) {
            initFieldNameIndexMap(sinkRecord);
        }
        DefaultContext<String, Object> context = new DefaultContext<>();
        for (Field field : fieldSet) {
            Object originFieldValue = ((Struct) sinkRecord.value()).get(field);
            context.put(field.name(), getComparableFieldValue(field.schema().name(), originFieldValue));
        }
        return (boolean) expressRunner.execute(express, context, null, true, true);
    }
    
    private Object getComparableFieldValue(final String fieldSchemaName, final Object originFieldValue) {
        if (fieldSchemaName != null) {
            switch (fieldSchemaName) {
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    return DateUtils.format((java.util.Date) originFieldValue, DateUtils.ZONE_UTC);
                case ZonedTimestamp.LOGICAL_NAME:
                    java.util.Date date = ZonedTimestamp.parseToDate((String) originFieldValue);
                    return DateUtils.format(date, timeZone.toZoneId());
                default:
            }
        }
        return originFieldValue;
    }
    
    private void initFieldNameIndexMap(final SinkRecord sinkRecord) {
        List<Field> fields = sinkRecord.valueSchema().fields();
        if (fields != null && fields.size() > 0) {
            fieldSet = fields.stream()
                    .filter(field -> Pattern.matches(getPattern(field.name()), condition))
                    .collect(Collectors.toSet());
            
            if (!Strings.isNullOrEmpty(condition) && fieldSet.isEmpty()) {
                throw new ConnectException(String.format("{} should include field name and field name should be space before and after: eg. (field_name_1 > 1)&&(field_name_2 > 2)",
                        SinkConfig.DESTINATIONS_CONFIG_PREFIX + destination + SinkConfig.DESTINATIONS_CONFIG_ROW_FILTER));
            }
        }
    }
    
    private String getPattern(final String key) {
        return "(.*(\\(|\\s)" + key + "(\\)|\\s).*|^" + key + "\\s.*|.*\\s" + key + "$)";
    }
}
