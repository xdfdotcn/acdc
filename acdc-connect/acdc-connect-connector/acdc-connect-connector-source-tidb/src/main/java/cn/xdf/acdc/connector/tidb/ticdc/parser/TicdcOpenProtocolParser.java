package cn.xdf.acdc.connector.tidb.ticdc.parser;

import cn.xdf.acdc.connector.tidb.reader.Event;
import cn.xdf.acdc.connector.tidb.reader.EventType;
import com.pingcap.ticdc.cdc.KafkaMessage;
import com.pingcap.ticdc.cdc.TicdcEventData;
import com.pingcap.ticdc.cdc.TicdcEventDecoder;
import com.pingcap.ticdc.cdc.value.TicdcEventDDL;
import com.pingcap.ticdc.cdc.value.TicdcEventResolve;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;

import java.util.ArrayList;
import java.util.List;

public class TicdcOpenProtocolParser {

    /**
     * Parse kafka message to event list.
     *
     * @param kafkaMessage kafka message
     * @return event list
     */
    public static List<Event> parse(final KafkaMessage kafkaMessage) {
        int order = 0;
        List<Event> result = new ArrayList<>();
        // https://github.com/pingcap/ticdc/tree/release-5.0/demo/java
        TicdcEventDecoder ticdcEventDecoder = new TicdcEventDecoder(kafkaMessage);
        while (ticdcEventDecoder.hasNext()) {
            TicdcEventData data = ticdcEventDecoder.next();
            if (data.getTicdcEventValue() instanceof TicdcEventRowChange) {
                result.add(new Event(EventType.ROW_CHANGED_EVENT, data, order++));
            } else if (data.getTicdcEventValue() instanceof TicdcEventDDL) {
                result.add(new Event(EventType.DDL_EVENT, data, order++));
            } else if (data.getTicdcEventValue() instanceof TicdcEventResolve) {
                result.add(new Event(EventType.RESOLVED_EVENT, data, order++));
            }
        }
        return result;
    }
}
