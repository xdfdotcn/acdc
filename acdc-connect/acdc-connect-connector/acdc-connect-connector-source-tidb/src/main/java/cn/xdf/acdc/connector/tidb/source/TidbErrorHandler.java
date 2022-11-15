package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.TidbConnector;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

public class TidbErrorHandler extends ErrorHandler {

    public TidbErrorHandler(final String logicalName, final ChangeEventQueue<?> queue) {
        super(TidbConnector.class, logicalName, queue);
    }

}
