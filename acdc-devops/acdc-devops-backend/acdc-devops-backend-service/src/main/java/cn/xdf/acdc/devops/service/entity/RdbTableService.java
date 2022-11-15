package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
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
 * Rdb table.
 */
public interface RdbTableService {

    /**
     * 单条保存.
     *
     * @param rdbTable RdbTable
     * @return RdbTable
     */
    RdbTableDO save(RdbTableDO rdbTable);

    /**
     * 批量创建.
     *
     * @param rdbTableList rdbTableList
     * @return List
     */
    List<RdbTableDO> saveAll(List<RdbTableDO> rdbTableList);

    /**
     * 动态条件分页列表.
     *
     * @param rdbTable 查询条件model类
     * @param pageable 分页设置
     * @return 分页列表
     */
    Page<RdbTableDO> query(RdbTableDO rdbTable, Pageable pageable);

    /**
     * 动态条件列表，不分页.
     *
     * @param rdbTable  rdbTable
     * @return 列表数据
     */
    List<RdbTableDO> queryAll(RdbTableDO rdbTable);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return RdbTable
     */
    Optional<RdbTableDO> findById(Long id);


    /**
     * Batch to obtain.
     * @param ids ids
     * @return List
     */
    List<RdbTableDO> findAllById(Iterable<Long> ids);

    /**
     * 动态条件.
     *
     * @param rdbTable 动态查询 model
     * @return 动态查询条件
     */
    static Specification specificationOf(final RdbTableDO rdbTable) {
        Preconditions.checkNotNull(rdbTable);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(rdbTable.getRdbDatabase()) && Objects.nonNull(rdbTable.getRdbDatabase().getId())) {
                predicates.add(cb.equal(root.get("rdbDatabase"), rdbTable.getRdbDatabase()));
            }
            if (StringUtils.isNotBlank(rdbTable.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", rdbTable.getName(), "%")));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
