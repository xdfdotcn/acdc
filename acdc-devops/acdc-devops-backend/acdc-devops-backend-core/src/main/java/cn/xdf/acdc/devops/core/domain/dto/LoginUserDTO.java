package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A CDC 登录用户信息.
 */
@Getter
@Setter
@Accessors(chain = true)
public class LoginUserDTO implements UserDetails {
    
    private final Long userid;
    
    private final String email;
    
    private final String domainAccount;
    
    private final String username;
    
    @JsonIgnore
    private final String password;
    
    private final List<GrantedAuthority> authorities;
    
    public LoginUserDTO(final UserDO user) {
        this.userid = user.getId();
        this.email = user.getEmail();
        this.domainAccount = user.getDomainAccount();
        this.username = user.getName();
        this.password = user.getPassword();
        
        this.authorities = user.getAuthorities().stream()
                .map(it -> new SimpleGrantedAuthority(it.getName()))
                .collect(Collectors.toList());
    }
    
    public LoginUserDTO(
            final Long userid,
            final String email,
            final String domainAccount,
            final String username,
            final String password,
            final Set<String> authorities
    ) {
        this.userid = userid;
        this.email = email;
        this.domainAccount = domainAccount;
        this.username = username;
        this.password = password;
        this.authorities = authorities.stream()
                .map(it -> new SimpleGrantedAuthority(it))
                .collect(Collectors.toList());
    }
    
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return this.authorities;
    }
    
    @Override
    public String getPassword() {
        return this.password;
    }
    
    @Override
    public String getUsername() {
        return this.username;
    }
    
    @Override
    public boolean isAccountNonExpired() {
        return false;
    }
    
    @Override
    public boolean isAccountNonLocked() {
        return false;
    }
    
    @Override
    public boolean isCredentialsNonExpired() {
        return false;
    }
    
    @Override
    public boolean isEnabled() {
        return true;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LoginUserDTO{");
        sb.append("id=").append(userid);
        sb.append(", email='").append(email).append('\'');
        sb.append(", domainAccount='").append(domainAccount).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", authorities=").append(authorities);
        sb.append('}');
        return sb.toString();
    }
}
