package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.dto.Connector;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.util.SpringUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.squirrelframework.foundation.fsm.StateMachineStatus;
import org.squirrelframework.foundation.fsm.impl.AbstractStateMachine;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ConnectorStateMachine extends AbstractStateMachine<ConnectorStateMachine, ConnectorState, ConnectorEvent, Connector> {

    private static final String RESOURCE_ACCESS_EXCEPTION_COUNTER_METRICS = "scheduler.statemachine.exception.connect.cluster.access";

    private static final String REQUEST_EXCEPTION_COUNTER_METRICS = "scheduler.statemachine.exception.connect.cluster.request";

    private static final String UNEXPECT_EXCEPTION_COUNTER_METRICS = "scheduler.statemachine.exception.unexpect";

    private static final String METRICS_LABEL_KEY_CONNECT_CLUSTER = "connect.cluster";

    private static final String DOT = ".";

    private static final Map<String, Counter> COUNTERS = new HashMap<>();

    @Override
    protected void afterTransitionCausedException(final ConnectorState from, final ConnectorState to, final ConnectorEvent event, final Connector connector) {
        Throwable exception = getLastException().getTargetException();
        if (exception instanceof ResourceAccessException) {
            log.error("ResourceAccessException: {}", exception.getMessage());
            counterMetrics(RESOURCE_ACCESS_EXCEPTION_COUNTER_METRICS, connector);
        } else if (exception instanceof HttpClientErrorException) {
            log.error("Request to connect cluster error: connectorId:{}, from:{}, to:{}, event:{}, exception:{}", connector.getId(), from, to, event, exception);
            counterMetrics(REQUEST_EXCEPTION_COUNTER_METRICS, connector);
        } else {
            log.error("State machine error: connectorId:{}, from:{}, to:{}, event:{}, exception:{}", connector.getId(), from, to, event, exception);
            counterMetrics(UNEXPECT_EXCEPTION_COUNTER_METRICS, connector);
        }
        setStatus(StateMachineStatus.IDLE);
    }

    private void counterMetrics(final String counterMetrics, final Connector connector) {
        String counterCacheKey = counterMetrics + DOT + connector.getConnectClusterUrl();
        if (!COUNTERS.containsKey(counterCacheKey)) {
            try {
                MeterRegistry meterRegistry = SpringUtils.getBean(MeterRegistry.class);
                Counter counter = Counter.builder(counterMetrics)
                        .tag(METRICS_LABEL_KEY_CONNECT_CLUSTER, connector.getConnectClusterUrl())
                        .register(meterRegistry);
                COUNTERS.put(counterCacheKey, counter);
            } catch (NullPointerException exception) {
                log.warn("Register counter metrics error: {}", exception);
            }
        }
        Counter counter = COUNTERS.get(counterCacheKey);
        if (counter != null) {
            counter.increment();
        }
    }
}
