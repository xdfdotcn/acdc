package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.UserAuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserAuthorityDTO {

    private Long userId;

    private AuthorityRoleType authorityRole;

    public UserAuthorityDTO(final UserAuthorityDO userAuthority) {
        this.userId = userAuthority.getUserId();
        this.authorityRole = userAuthority.getAuthorityName();
    }
}
