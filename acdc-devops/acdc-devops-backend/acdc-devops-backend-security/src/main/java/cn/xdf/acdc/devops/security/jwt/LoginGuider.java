package cn.xdf.acdc.devops.security.jwt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.security.core.userdetails.UserDetails;

@JsonInclude(Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Accessors(chain = true)
public final class LoginGuider {

    private UserDetails user;

    private String url;

    private String token;
}
