package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Accessors(chain = true)
public class PagedQuery {

    private static final int MAX_PAGE_SIZE = 100;

    private static final String DEFAULT_SORT_FIELD = "id";

    private int current;

    private int pageSize;

    /**
     * 生成 spring 底层依赖的 pageable 分页对象, 默认排序字段为ID.
     *
     * @param current  当前页面,从1开始
     * @param pageSize 分页大小
     * @return Pageable
     */
    public static Pageable pageOf(final int current, final int pageSize) {
        return pageOf(current, pageSize, DEFAULT_SORT_FIELD);
    }

    /**
     * 生成 spring 底层依赖的 pageable 分页对象, 默认排序字段为ID.
     *
     * @param pagedQuery pagedQuery 对象
     * @return Pageable
     */
    public static Pageable pageOf(final PagedQuery pagedQuery) {
        return pageOf(pagedQuery.getCurrent(), pagedQuery.getPageSize(), DEFAULT_SORT_FIELD);
    }

    /**
     * 生成 spring 底层依赖的 pageable 分页对象, 默认排序字段为ID.
     *
     * @param current   当前页面,从1开始
     * @param pageSize  分页大小
     * @param sortField 排序字段
     * @return Pageable
     */
    public static Pageable pageOf(final int current, final int pageSize, final String sortField) {
        int fixCurrent = current <= 0 ? 0 : current - 1;
        int fixPageSize = MAX_PAGE_SIZE - pageSize > 0 ? Math.max(1, pageSize) : MAX_PAGE_SIZE;
        return PageRequest.of(fixCurrent, fixPageSize, Sort.by(Sort.Order.desc(sortField)));
    }

}
