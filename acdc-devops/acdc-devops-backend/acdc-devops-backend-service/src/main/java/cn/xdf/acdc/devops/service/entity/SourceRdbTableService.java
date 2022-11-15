package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.CreationResult;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.util.CollectionUtils;

/**
 * Source rdb table.
 *
 */
public interface SourceRdbTableService {

    /**
     * 单条保存.
     *
     * @param sourceRdbTable SourceRdbTable
     * @return SourceRdbTable
     */
    SourceRdbTableDO save(SourceRdbTableDO sourceRdbTable);

    /**
     * 单条保存.
     *
     * @param rdbTableId rdbTableId
     * @param connectorId connectorId
     * @param topic topic
     * @return SourceRdbTable
     */
    SourceRdbTableDO save(Long rdbTableId, Long connectorId, String topic);

    /**
     * 单条保存.
     *
     * @param rdbTableId rdbTableId
     * @param connectorId connectorId
     * @param topic topic
     * @return SourceRdbTable
     */
    CreationResult<SourceRdbTableDO> saveIfAbsent(Long rdbTableId, Long connectorId, String topic);

    /**
     * 批量保存.
     *
     * @param sourceRdbTableList sourceRdbTableList
     * @return sourceRdbTableList
     */
    List<SourceRdbTableDO> saveAll(List<SourceRdbTableDO> sourceRdbTableList);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<SourceRdbTableDO> findAll();

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return SourceRdbTable
     */
    Optional<SourceRdbTableDO> findById(Long id);

    /**
     * 根据 Connector ID查询.
     *
     * @param connectorId Connector ID
     * @return SourceRdbTable
     */
    List<SourceRdbTableDO> findByConnectorId(Long connectorId);

    /**
     * 根据 rdbTableId 查询.
     *
     * @param rdbTableId rdbTableId
     * @return SourceRdbTable
     */
    Optional<SourceRdbTableDO> findByRdbTableId(Long rdbTableId);

    /**
     * 根据 KafkaTopic 查询.
     *
     * @param kafkaTopicId kafkaTopicId
     * @return SourceRdbTable
     */
    Optional<SourceRdbTableDO> findByKafkaTopicId(Long kafkaTopicId);

    /**
     * 动态条件列表，不分页.
     *
     * @param rdbTableIdList rdbTableIdList
     * @return List
     */
    List<SourceRdbTableDO> queryByRdbTableIdList(List<Long> rdbTableIdList);

    /**
     * 动态条件.
     *
     * @param rdbTableIdList rdbTableIdList
     * @return 动态查询条件
     */
    static Specification specificationOf(final List<Long> rdbTableIdList) {

        Preconditions.checkArgument(!CollectionUtils.isEmpty(rdbTableIdList));

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            CriteriaBuilder.In in = cb.in(root.get("rdbTable"));
            for (Long id : rdbTableIdList) {
                in.value(id);
            }
            predicates.add(in);

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
