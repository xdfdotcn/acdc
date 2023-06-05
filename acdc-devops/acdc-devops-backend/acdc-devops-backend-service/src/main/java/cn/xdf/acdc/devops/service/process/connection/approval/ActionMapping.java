package cn.xdf.acdc.devops.service.process.connection.approval;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.approval.action.ApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;
import java.util.Optional;

// CHECKSTYLE:OFF

/**
 * Declaration action mapping .
 */
@Getter
public final class ActionMapping {
    
    // from state
    private final ApprovalState from;
    
    // to state
    private final ApprovalState to;
    
    // trigger event
    private final ApprovalEvent event;
    
    // action
    private final Class<? extends ApprovalAction> actionClass;
    
    private ActionMapping(
            final ApprovalState from,
            final ApprovalState to,
            final ApprovalEvent event,
            final Class<? extends ApprovalAction> actionClass
    ) {
        this.from = from;
        this.to = to;
        this.event = event;
        this.actionClass = actionClass;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        
        private final Map<ActionMappingKey, ActionMapping> actionMapping = Maps.newHashMap();
        
        public Builder add(
                final ApprovalState from,
                final ApprovalState to,
                final ApprovalEvent event,
                final Class<? extends ApprovalAction> actionClass
        ) {
            ActionMappingKey key = ActionMappingKey.of(from, event);
            this.actionMapping.put(key, new ActionMapping(from, to, event, actionClass));
            return this;
        }
        
        public Map<ActionMappingKey, ActionMapping> build() {
            return Optional.of(actionMapping).get();
        }
    }
    
    @Getter
    public final static class ActionMappingKey {
        
        private ApprovalState from;
        
        private ApprovalEvent event;
        
        public ActionMappingKey(final ApprovalState from, final ApprovalEvent event) {
            this.from = from;
            this.event = event;
        }
        
        public static ActionMappingKey of(final ApprovalState from, final ApprovalEvent event) {
            return new ActionMappingKey(from, event);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ActionMappingKey that = (ActionMappingKey) o;
            return from == that.from && event == that.event;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(from, event);
        }
    }
}
