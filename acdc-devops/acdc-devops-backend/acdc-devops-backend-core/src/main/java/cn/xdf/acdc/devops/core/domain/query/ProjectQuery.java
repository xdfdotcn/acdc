package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProjectQuery extends PagedQuery {

    public enum RANGE {
        ALL, CURRENT_USER
    }

    private Long id;

    private String name;

    private String description;

    private Long owner;

    private Integer source;

    private Long originalId;

    private Instant creationTime;

    private RANGE queryRange;

    private Set<Long> projectIds;
}
