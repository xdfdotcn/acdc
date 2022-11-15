package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserAuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.util.UserUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder

public class UserDetailDTO {

    private Long id;

    private String email;

    private String domainAccount;

    private String name;

    @JsonIgnore
    private String password;

    private Set<AuthorityRoleType> authoritySet;

    public UserDetailDTO(final UserDO userDO) {
        this.id = userDO.getId();
        this.email = userDO.getEmail();
        this.domainAccount = userDO.getDomainAccount();
        this.name = userDO.getName();
        this.password = userDO.getPassword();
        this.authoritySet = userDO.getAuthorities().stream().map(it -> AuthorityRoleType.valueOf(it.getName())).collect(Collectors.toSet());
    }

    /**
     * To UserDO.
     *
     * @return UserDO
     */
    public UserDO toUserDO() {
        return UserDO.builder()
                .id(this.id)
                .name(this.name)
                .email(this.email)
                .password(password)
                .createdBy(SystemConstant.ACDC)
                .updatedBy(SystemConstant.ACDC)
                .authorities(toAuthorityDOSet())
                .domainAccount(UserUtil.convertEmailToDomainAccount(this.email))
                .build();
    }

    /**
     * To user authority set.
     *
     * @return authority set
     */
    public Set<UserAuthorityDO> toUserAuthorityDOSet() {
        if (CollectionUtils.isEmpty(authoritySet)) {
            return Collections.EMPTY_SET;
        }
        return authoritySet.stream()
                .map(it -> new UserAuthorityDO(this.id, it))
                .collect(Collectors.toSet());
    }

    /**
     * To AuthorityDO set.
     *
     * @return Set
     */
    public Set<AuthorityDO> toAuthorityDOSet() {
        Set<AuthorityRoleType> newAuthorityRoleTypes = CollectionUtils.isEmpty(authoritySet)
                ? new HashSet<>() : authoritySet;
        newAuthorityRoleTypes.add(AuthorityRoleType.ROLE_USER);
        if (CollectionUtils.isEmpty(authoritySet)) {
            return Collections.EMPTY_SET;
        }
        return authoritySet.stream()
                .map(it -> new AuthorityDO(it.name()))
                .collect(Collectors.toSet());
    }
}
