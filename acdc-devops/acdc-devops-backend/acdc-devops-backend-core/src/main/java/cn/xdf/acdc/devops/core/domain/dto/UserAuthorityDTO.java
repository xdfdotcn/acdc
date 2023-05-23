package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.UserAuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class UserAuthorityDTO {
    
    private Long userId;
    
    private AuthorityRoleType authorityRole;
    
    public UserAuthorityDTO(final UserAuthorityDO userAuthority) {
        this.userId = userAuthority.getUserId();
        this.authorityRole = userAuthority.getAuthorityName();
    }
}
