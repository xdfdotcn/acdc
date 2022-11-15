package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import io.jsonwebtoken.lang.Assert;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.persistence.criteria.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

/**
 * RDB 实例 service.
 */
public interface RdbInstanceService {

    /**
     * 创建实例.
     *
     * @param rdbInstance RdbInstance
     * @return 插入数据库成功的 RdbInstance
     */
    RdbInstanceDO save(RdbInstanceDO rdbInstance);

    /**
     * 批量创建数据库实例.
     *
     * @param rdbInstanceList 批量数据库实例集合
     * @return 插入成功的 RdbInstance 集合
     */
    List<RdbInstanceDO> saveAll(List<RdbInstanceDO> rdbInstanceList);

    /**
     * 根据条件,获取分页的数据库实例列表.
     *
     * @param rdbInstance  查询条件model类
     * @param pageable 分页设置
     * @return 数据库实例分页列表
     */
    Page<RdbInstanceDO> query(RdbInstanceDO rdbInstance, Pageable pageable);

    /**
     * 查询数据库实例列表，不分页.
     *
     * @param rdbInstance rdbInstance
     * @return 数据库实力列表
     */
    List<RdbInstanceDO> queryAll(RdbInstanceDO rdbInstance);

    /**
     * 根据ID查询数据库实例.
     *
     * @param id id
     * @return RdbInstance
     */
    Optional<RdbInstanceDO> findById(Long id);

    /**
     * Batch to obtain.
     * @param ids ids
     * @return List
     */
    List<RdbInstanceDO> findAllById(Iterable<Long> ids);

    /**
     * 根据集群 ID,host,port 查询实例.
     *
     * @param rdbId  rdbId
     * @param host  host
     * @param port  port
     * @return RdbInstance
     */
    Optional<RdbInstanceDO> findByRdbIdAndHostAndPort(Long rdbId, String host, Integer port);

    /**
     * 根据 rdbId 查询 data source 实例.
     *
     * @param rdbId  rdbId
     * @return RdbInstance
     */
    Optional<RdbInstanceDO> findDataSourceInstanceByRdbId(Long rdbId);

    /**
     * 动态条件查询，根据传入的 model 动态查询.
     *
     * @param rdbInstance 动态查询model
     * @return 动态查询条件
     */
    static Specification specificationOf(final RdbInstanceDO rdbInstance) {
        Assert.notNull(rdbInstance);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            // host
            if (StringUtils.isNotBlank(rdbInstance.getHost())) {
                predicates.add(cb.like(root.get("host"), QueryUtil.like("%", rdbInstance.getHost(), "%")));
            }

            // instance id
            if (Objects.nonNull(rdbInstance.getId())) {
                predicates.add(cb.equal(root.get("id"), rdbInstance.getId()));
            }
            // rdb
            if (Objects.nonNull(rdbInstance.getRdb())
                && Objects.nonNull(rdbInstance.getRdb().getId())
            ) {
                predicates.add(cb.equal(root.get("rdb"), rdbInstance.getRdb()));
            }

            // role
            if (Objects.nonNull(rdbInstance.getRole())) {
                predicates.add(cb.equal(root.get("role"), rdbInstance.getRole()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
