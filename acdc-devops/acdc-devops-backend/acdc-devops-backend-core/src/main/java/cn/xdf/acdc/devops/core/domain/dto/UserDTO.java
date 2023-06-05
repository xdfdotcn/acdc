package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class UserDTO {
    
    private Long id;
    
    private String email;
    
    private Integer ownerFlag;
    
    private String domainAccount;
    
    private String name;
    
    private String password;
    
    public UserDTO(final UserDO user) {
        this.id = user.getId();
        // Customize it here if you need, or not, firstName/lastName/etc
        this.domainAccount = user.getDomainAccount();
        this.email = user.getEmail();
        this.name = user.getName();
        this.password = user.getPassword();
    }
    
    /**
     * To UserDO.
     *
     * @return UserDO
     */
    public UserDO toDO() {
        UserDO userDO = new UserDO();
        userDO.setId(this.id);
        userDO.setEmail(this.email);
        userDO.setDomainAccount(this.domainAccount);
        userDO.setPassword(this.password);
        userDO.setName(this.name);
        userDO.setCreatedBy(SystemConstant.ACDC);
        userDO.setUpdatedBy(SystemConstant.ACDC);
        return userDO;
    }
}
