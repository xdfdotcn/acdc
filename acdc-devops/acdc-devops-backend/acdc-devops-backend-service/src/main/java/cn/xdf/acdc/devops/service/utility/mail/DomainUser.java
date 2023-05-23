package cn.xdf.acdc.devops.service.utility.mail;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DomainUser {
    
    private String email;
    
    private String domainAccount;
    
    private String briefDomainAccount;
    
    private String username;
    
    public DomainUser(final String email, final String username) {
        String domainAccount = Splitter.on(SystemConstant.Symbol.AT).splitToList(email).get(0);
        this.email = email;
        this.domainAccount = domainAccount;
        this.username = username;
        this.briefDomainAccount = CharMatcher.digit().removeFrom(domainAccount);
    }
    
    public DomainUser(final String email) {
        String domainAccount = Splitter.on(SystemConstant.Symbol.AT).splitToList(email).get(0);
        this.email = email;
        this.domainAccount = domainAccount;
        this.username = SystemConstant.EMPTY_STRING;
        this.briefDomainAccount = CharMatcher.digit().removeFrom(domainAccount);
    }
}
