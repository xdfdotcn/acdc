package cn.xdf.acdc.devops.service.aop;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.lang.reflect.Method;

@Component
@Aspect
public class EventAspect {

    private final ExpressionParser parser = new SpelExpressionParser();

    private final LocalVariableTableParameterNameDiscoverer discoverer = new LocalVariableTableParameterNameDiscoverer();

    private final ConnectorEventService connectorEventService;

    public EventAspect(final ConnectorEventService connectorEventService) {
        this.connectorEventService = connectorEventService;
    }

    /**
     * Advice that save connector event when annotation with @Event.
     *
     * @param joinPoint join point for advice.
     * @param event     event annotation.
     * @return result.
     * @throws Throwable throws.
     */
    @Around("@annotation(event)")
    public Object around(final ProceedingJoinPoint joinPoint, final Event event) throws Throwable {
        Object result;

        Object[] args = joinPoint.getArgs();
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();

        EvaluationContext context = new MethodBasedEvaluationContext(null, method, args, discoverer);

        Long connectorId = (Long) getSpelValue(event.connectorId(), context, Long.class);
        String message = (String) getSpelValue(event.message(), context, String.class);
        String level = (String) getSpelValue(event.level(), context, String.class);

        try {
            result = joinPoint.proceed();
            saveEvent(connectorId, event.reason().getName(), message, event.source(), EventLevel.valueOf(level));
        } catch (ResourceAccessException | HttpClientErrorException e) {
            saveEvent(connectorId, EventReason.EXECUTION_ERROR.getName(), e + "\n" + e.getStackTrace()[0],
                    event.source(), EventLevel.ERROR);
            throw e;
        }
        return result;
    }

    private void saveEvent(final Long connectorId, final String reason, final String message, final EventSource source, final EventLevel level) {
        ConnectorEventDO connectorEvent = ConnectorEventDO.builder()
                .reason(reason)
                .message(message)
                .source(source)
                .level(level)
                .connector(ConnectorDO.builder()
                        .id(connectorId)
                        .build())
                .build();

        connectorEventService.save(connectorEvent);
    }

    private Object getSpelValue(final String spel, final EvaluationContext context, final Class clz) {
        Expression expression = parser.parseExpression(spel);
        return expression.getValue(context, clz);
    }
}
