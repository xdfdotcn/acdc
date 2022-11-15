package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
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
 * Rdb database.
 */
public interface RdbDatabaseService {

    /**
     * 创建数据库.
     *
     * @param rdbDatabase RdbDataBase
     * @return 插入数据库成功的 RdbDataBase
     */
    RdbDatabaseDO save(RdbDatabaseDO rdbDatabase);

    /**
     * 批量创建数据库.
     *
     * @param rdbDatabaseList RdbDataBase集合
     * @return 插入数据库成功的项目集合
     */
    List<RdbDatabaseDO> saveAll(List<RdbDatabaseDO> rdbDatabaseList);

    /**
     * 根据条件,获取分页的数据库列表.
     *
     * @param rdbDatabase 查询条件model类
     * @param pageable    分页设置
     * @return 项目分页列表
     */
    Page<RdbDatabaseDO> query(RdbDatabaseDO rdbDatabase, Pageable pageable);

    /**
     * 查询数据库列表，不分页.
     *
     * @param rdbDatabase rdbDatabase
     * @return 所有项目列表
     */
    List<RdbDatabaseDO> queryAll(RdbDatabaseDO rdbDatabase);

    /**
     * 根据 ID 查询.
     *
     * @param rdbDatabaseId ID
     * @return RdbDatabase
     */
    Optional<RdbDatabaseDO> findById(Long rdbDatabaseId);

    /**
     * 动态条件.
     *
     * @param rdbDatabase  rdbDatabase
     * @return Specification
     */
    static Specification specificationOf(final RdbDatabaseDO rdbDatabase) {
        Preconditions.checkNotNull(rdbDatabase);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(rdbDatabase.getRdb())
                && Objects.nonNull(rdbDatabase.getRdb().getId())
            ) {
                predicates.add(cb.equal(root.get("rdb"), rdbDatabase.getRdb()));
            }

            if (StringUtils.isNotBlank(rdbDatabase.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", rdbDatabase.getName(), "%")));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
