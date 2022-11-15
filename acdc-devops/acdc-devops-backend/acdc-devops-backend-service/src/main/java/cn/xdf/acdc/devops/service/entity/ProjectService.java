package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.util.CollectionUtils;

/**
 * Project.
 */
public interface ProjectService {

    /**
     * 创建项目.
     *
     * @param project 项目model类
     * @return 插入数据库成功的 Project
     */
    ProjectDO save(ProjectDO project);

    /**
     * 批量创建项目.
     *
     * @param projectList 批量创建的项目集合
     * @return 插入数据库成功的项目集合
     */
    List<ProjectDO> saveAll(List<ProjectDO> projectList);

    /**
     * 根据条件,获取分页的项目列表.
     *
     * @param projectQuery 查询条件model类
     * @param pageable 分页设置
     * @return 项目分页列表
     */
    Page<ProjectDO> query(ProjectQuery projectQuery, Pageable pageable);

    /**
     * 根据ID查询项目.
     *
     * @param id 主键
     * @return 项目
     */
    Optional<ProjectDO> findById(Long id);

    /**
     * Batch to obtain.
     *
     * @param ids ids
     * @return List
     */
    List<ProjectDO> findAllById(Iterable<Long> ids);

    /**
     * 查询所有项目.
     *
     * @param projectQuery projectQuery
     * @return 所有项目列表
     */
    List<ProjectDO> queryAll(ProjectQuery projectQuery);

    /**
     * 动态条件查询，根据传入的 model 动态查询.
     *
     * @param projectQuery 动态查询model
     * @return 动态查询条件
     */
    static Specification specificationOf(final ProjectQuery projectQuery) {
        Preconditions.checkNotNull(projectQuery);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(projectQuery.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", projectQuery.getName(), "%")));
            }

            if (!CollectionUtils.isEmpty(projectQuery.getProjectIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : projectQuery.getProjectIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
