package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * 分页配置,与前端AntD 字段名称对应.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PageDTO<T> {

    private List<T> data;

    private long total;

    private int current;

    private int pageSize;

    public PageDTO(final List<T> data, final long total) {
        this.data = data;
        this.total = total;
    }

    /**
     * 空数据.
     * @param <E> E
     * @return PageDTO
     */
    public static <E> PageDTO<E> empty() {
        return new PageDTO(Collections.EMPTY_LIST, 0L);
    }

    /**
     * 根据数据集合和总条数转换成分页对象.
     * @param <E> E
     * @param data data
     * @param total total
     * @return PageDTO
     */
    public static <E> PageDTO<E> of(final List<E> data, final long total) {
        return new PageDTO<>(data, total);
    }
}
