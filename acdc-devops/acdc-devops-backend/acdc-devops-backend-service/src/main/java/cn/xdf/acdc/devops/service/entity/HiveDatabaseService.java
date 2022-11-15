package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.base.Preconditions;
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
 * Hive database.
 */
public interface HiveDatabaseService {

    /**
     * 单条保存.
     *
     * @param hiveDatabase hiveDatabase
     * @return HiveDatabase
     */
    HiveDatabaseDO save(HiveDatabaseDO hiveDatabase);

    /**
     * 批量保存.
     *
     * @param hiveDatabaseList hiveDatabaseList
     * @return List
     */
    List<HiveDatabaseDO> saveAll(List<HiveDatabaseDO> hiveDatabaseList);

    /**
     * 分页列表.
     *
     * @param hiveDatabase hiveDatabase
     * @param pageable pageable
     * @return Page
     */
    Page<HiveDatabaseDO> query(HiveDatabaseDO hiveDatabase, Pageable pageable);

    /**
     * 查询数据库列表，不分页.
     *
     * @param hiveDatabase hiveDatabase
     * @return List
     */
    List<HiveDatabaseDO> queryAll(HiveDatabaseDO hiveDatabase);

    /**
     * 根据 ID 查询.
     *
     * @param hiveDatabaseId ID
     * @return HiveDatabase
     */
    Optional<HiveDatabaseDO> findById(Long hiveDatabaseId);

    /**
     * 动态条件.
     *
     * @param hiveDatabase hiveDatabase
     * @return Specification
     */
    static Specification specificationOf(final HiveDatabaseDO hiveDatabase) {
        Preconditions.checkNotNull(hiveDatabase);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(hiveDatabase.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", hiveDatabase.getName(), "%")));
            }

            if (Objects.nonNull(hiveDatabase.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), hiveDatabase.getDeleted()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
