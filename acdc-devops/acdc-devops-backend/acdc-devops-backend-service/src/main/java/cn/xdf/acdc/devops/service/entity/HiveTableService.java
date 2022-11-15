package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
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
 * Hive table.
 *
 */
public interface HiveTableService {

    /**
     * 单条保存.
     *
     * @param hiveTable HiveTable
     * @return HiveTable
     */
    HiveTableDO save(HiveTableDO hiveTable);

    /**
     * 批量创建.
     *
     * @param hiveTableList hiveTableList
     * @return List
     */
    List<HiveTableDO> saveAll(List<HiveTableDO> hiveTableList);

    /**
     * 动态条件分页列表.
     *
     * @param hiveTable 查询条件model类
     * @param pageable  分页设置
     * @return 分页列表
     */
    Page<HiveTableDO> query(HiveTableDO hiveTable, Pageable pageable);

    /**
     * 动态条件列表，不分页.
     *
     * @param hiveTable hiveTable
     * @return 列表数据
     */
    List<HiveTableDO> queryAll(HiveTableDO hiveTable);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return HiveTable
     */
    Optional<HiveTableDO> findById(Long id);

    /**
     * Batch to obtain.
     * @param ids ids
     * @return List
     */
    List<HiveTableDO> findAllById(Iterable<Long> ids);

    /**
     * 动态条件.
     *
     * @param hiveTable 动态查询 model
     * @return 动态查询条件
     */
    static Specification specificationOf(final HiveTableDO hiveTable) {
        Preconditions.checkNotNull(hiveTable);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(hiveTable.getHiveDatabase())
                && Objects.nonNull(hiveTable.getHiveDatabase().getId())
            ) {
                predicates.add(cb.equal(root.get("hiveDatabase"), hiveTable.getHiveDatabase()));
            }
            if (StringUtils.isNotBlank(hiveTable.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", hiveTable.getName(), "%")));
            }

            if (Objects.nonNull(hiveTable.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), hiveTable.getDeleted()));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
