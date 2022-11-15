package cn.xdf.acdc.devops.statemachine;

import io.micrometer.core.annotation.Timed;

@FunctionalInterface
public interface EventHandleConsumer {

    /**
     * Performs this operation on the given argument.
     *
     * @param clusterId kafka cluster id
     * @param userTriggerConnectorEvent user trigger connector event
     */
    @Timed(description = "user trigger event")
    void accept(Long clusterId, UserTriggerConnectorEvent userTriggerConnectorEvent);

}
