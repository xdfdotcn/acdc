package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import com.google.common.base.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashSet;
import java.util.Set;

/**
 * 数据表字段.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RelationalDatabaseTableField {

    private String name;

    private String type;

    @Builder.Default
    private Set<String> uniqueIndexNames = new HashSet<>();

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RelationalDatabaseTableField)) {
            return false;
        }

        RelationalDatabaseTableField other = (RelationalDatabaseTableField) obj;

        return Objects.equal(name, other.name) && Objects.equal(type, other.type);
    }
}
