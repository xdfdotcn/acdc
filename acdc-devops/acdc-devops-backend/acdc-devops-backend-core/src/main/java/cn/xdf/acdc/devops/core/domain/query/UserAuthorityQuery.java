package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class UserAuthorityQuery {
    
    private Set<AuthorityRoleType> authorityRoleTypes;
    
}
