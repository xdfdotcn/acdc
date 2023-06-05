package cn.xdf.acdc.devops.service.aop;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Event {
    
    /**
     * Connector id.
     *
     * @return connector id
     */
    String connectorId();
    
    /**
     * Event reason.
     *
     * @return event reason
     */
    EventReason reason();
    
    /**
     * Event source.
     *
     * @return event source
     */
    EventSource source();
    
    /**
     * Event level, eg. INFO, TRACE, ERROR.
     *
     * @return event level
     */
    String level() default "'INFO'";
    
    /**
     * Event message.
     *
     * @return event message
     */
    String message() default "''";
}
