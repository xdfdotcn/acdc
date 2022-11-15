package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.CreationResult;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Connector.
 */
public interface ConnectorService {

    /**
     * 单条保存.
     *
     * @param connector Connector
     * @return Connector
     */
    ConnectorDO save(ConnectorDO connector);

    /**
     * 单条保存.
     *
     * @param connectorName connectorName
     * @param  dataSystemType dataSystemType
     * @param connectorType connectorType
     * @return Connector
     */
    ConnectorDO save(String connectorName, DataSystemType dataSystemType, ConnectorType connectorType);

    /**
     * 单条保存.
     *
     * @param dataBaseId dataBaseId
     * @param connectorName  connectorName
     * @param dataSystemType dataSystemType
     * @param connectorType connectorType
     * @return Connector
     */
    CreationResult<ConnectorDO> saveSourceIfAbsent(Long dataBaseId, String connectorName, DataSystemType dataSystemType, ConnectorType connectorType);


    /**
     * 批量创建.
     *
     * @param connectorList connectorList
     * @return List
     */
    List<ConnectorDO> saveAll(List<ConnectorDO> connectorList);

    /**
     * Page query.
     *
     * @param connectorQuery connectorQuery
     * @param pageable  pageable
     * @return Page
     */
    Page<ConnectorDO> pageQuery(ConnectorQuery connectorQuery, Pageable pageable);

    /**
     * Query List.
     *
     * @param query connectorQuery
     * @return Page
     */
    List<ConnectorDO> query(ConnectorQuery query);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return Connector
     */
    Optional<ConnectorDO> findById(Long id);

    /**
     * 动态条件.
     *
     * @param connectionQuery connectionQuery
     * @return 动态查询条件
     */
    static Specification specificationOf(final ConnectorQuery connectionQuery) {
        Preconditions.checkNotNull(connectionQuery);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(connectionQuery.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", connectionQuery.getName(), "%")));
            }

            if (Objects.nonNull(connectionQuery.getConnectCluster()) && Objects.nonNull(connectionQuery.getConnectCluster().getId())) {
                predicates.add(cb.equal(root.get("connectCluster"), connectionQuery.getConnectCluster()));
            }

            if (Objects.nonNull(connectionQuery.getActualState())) {
                predicates.add(cb.equal(root.get("actualState"), connectionQuery.getActualState()));
            }

            if (Objects.nonNull(connectionQuery.getDesiredState())) {
                predicates.add(cb.equal(root.get("desiredState"), connectionQuery.getDesiredState()));
            }

            if (Objects.nonNull(connectionQuery.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), connectionQuery.getBeginUpdateTime()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

}
