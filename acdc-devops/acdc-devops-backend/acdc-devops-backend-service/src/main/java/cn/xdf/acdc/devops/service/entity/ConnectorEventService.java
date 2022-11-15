package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorEventQuery;
import cn.xdf.acdc.devops.core.util.DateUtil;
import com.google.common.base.Preconditions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.criteria.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

/**
 * Connector event.
 */
public interface ConnectorEventService {

    /**
     * Save connector event.
     *
     * @param connectorEvent ConnectorEvent
     * @return ConnectorEvent
     */
    ConnectorEventDO save(ConnectorEventDO connectorEvent);

    /**
     * Find by connector id.
     *
     * @param connectorId connector id.
     * @return connector event list.
     */
    List<ConnectorEventDO> findByConnectorId(Long connectorId);

    /**
     * Get all connector events.
     *
     * @return all connector events
     */
    List<ConnectorEventDO> findAll();

    /**
     * Paging query event.
     *
     * @param query    query
     * @param pageable pageable
     * @return Page
     */
    Page<ConnectorEventDO> query(ConnectorEventQuery query, Pageable pageable);


    /**
     * Dynamic conditions.
     *
     * @param eventQuery 动态查询 model
     * @return 动态查询条件
     */
    static Specification specificationOf(final ConnectorEventQuery eventQuery) {
        Preconditions.checkNotNull(eventQuery);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(eventQuery.getConnectorId())) {
                predicates.add(cb.equal(root.get("connector"), ConnectorDO.builder().id(eventQuery.getConnectorId()).build()));
            }

            if (Objects.nonNull(eventQuery.getSource())) {
                predicates.add(cb.equal(root.get("source"), eventQuery.getSource()));
            }

            if (Objects.nonNull(eventQuery.getLevel())) {
                predicates.add(cb.equal(root.get("level"), eventQuery.getLevel()));
            }

            if (StringUtils.isNotBlank(eventQuery.getReason())) {
                predicates.add(cb.equal(root.get("reason"), eventQuery.getReason()));
            }

            if (StringUtils.isNotBlank(eventQuery.getBeginTime())
                    && StringUtils.isNotBlank(eventQuery.getEndTime())
            ) {
                Instant begin = DateUtil.parseToInstant(eventQuery.getBeginTime());
                Instant end = DateUtil.parseToInstant(eventQuery.getEndTime());
                predicates.add(cb.between(root.get("creationTime"), begin, end));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
