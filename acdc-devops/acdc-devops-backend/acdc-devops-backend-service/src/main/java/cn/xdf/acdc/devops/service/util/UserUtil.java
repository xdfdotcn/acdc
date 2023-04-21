package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import com.google.common.base.Preconditions;
import org.springframework.security.core.GrantedAuthority;

public class UserUtil {

    protected static final String EMAIL_SUFFIX = "@xdf.cn";

    /**
     * Convert to domain account to email.
     *
     * @param domainAccount domain account
     * @return email
     */
    public static String convertDomainAccountToEmail(final String domainAccount) {
        Preconditions.checkNotNull(domainAccount);
        return String.format("%s%s", domainAccount, EMAIL_SUFFIX);
    }

    /**
     * convert email to domain account.
     *
     * @param email email
     * @return domain account
     */
    public static String convertEmailToDomainAccount(final String email) {
        Preconditions.checkNotNull(email);
        int index = email.lastIndexOf(EMAIL_SUFFIX);
        if (index == -1) {
            return email;
        }
        return email.substring(0, index);
    }

    /**
     * Check if the user is admin or not.
     *
     * @param user user to check
     * @return true if the user is admin, otherwise false.
     */
    public static boolean isAdmin(final UserDO user) {
        for (AuthorityDO authorityDO : user.getAuthorities()) {
            if (AuthorityRoleType.ROLE_ADMIN.name().equals(authorityDO.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the user is admin or not.
     *
     * @param user user to check
     * @return true if the user is admin, otherwise false.
     */
    public static boolean isAdmin(final LoginUserDTO user) {
        for (GrantedAuthority authority : user.getAuthorities()) {
            if (AuthorityRoleType.ROLE_ADMIN.name().equals(authority.getAuthority())) {
                return true;
            }
        }
        return false;
    }
}
