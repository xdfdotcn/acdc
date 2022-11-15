package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DomainUserDTO {

    private String email;

    private String domainAccount;

    private String briefDomainAccount;

    private String userName;

    public DomainUserDTO(final UserDO userDO) {
        this.email = userDO.getEmail();
        this.userName = userDO.getName();
        this.domainAccount = userDO.getDomainAccount();
        this.briefDomainAccount = CharMatcher.digit().removeFrom(userDO.getDomainAccount());
    }

    public DomainUserDTO(final String email) {
        String domainAccount = Splitter.on(SystemConstant.Symbol.AT).splitToList(email).get(0);
        this.email = email;
        this.domainAccount = domainAccount;
        this.briefDomainAccount = CharMatcher.digit().removeFrom(domainAccount);
    }
}
