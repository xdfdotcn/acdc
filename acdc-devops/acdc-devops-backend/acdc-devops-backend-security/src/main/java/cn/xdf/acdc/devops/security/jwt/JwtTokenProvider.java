package cn.xdf.acdc.devops.security.jwt;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.security.Key;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class JwtTokenProvider {

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String AUTHORITIES_KEY = "auth";

    private static final String USER_ID_KEY = "id";

    private static final String USER_NAME_KEY = "username";

    private static final String USER_EMAIL_KEY = "email";

    private static final String USER_DOMAIN_ACCOUNT_KEY = "domainAccount";

    private static final String BASE64_SECRET = "hvqHpEdlkeljo0ByrOpdEQ5Vq12WZj80ZI3J4pnODYQsekCVrFYbhs7DlSlOR3cs0s7Cfl0P8GdqNfXtibUIp0M4eGXxLdWEYYm";

    private static final Long TOKEN_VALIDITY_IN_SECONDS = 60 * 30L;

    private static final Long TOKEN_VALIDITY_IN_SECONDS_FOR_REMEMBER_ME = 60 * 60 * 6L;

    private final Key key;

    private final JwtParser jwtParser;

    private final long tokenValidityInMilliseconds;

    private final long tokenValidityInMillisecondsForRememberMe;

    public JwtTokenProvider() {
        byte[] keyBytes = Decoders.BASE64.decode(BASE64_SECRET);
        key = Keys.hmacShaKeyFor(keyBytes);
        jwtParser = Jwts.parserBuilder().setSigningKey(key).build();
        this.tokenValidityInMilliseconds = 1000 * TOKEN_VALIDITY_IN_SECONDS;
        this.tokenValidityInMillisecondsForRememberMe = 1000 * TOKEN_VALIDITY_IN_SECONDS_FOR_REMEMBER_ME;
    }

    /**
     * 生成 Token.
     *
     * @param user       user
     * @param rememberMe rememberMe
     * @return token string
     */
    public String createToken(final LoginUserDTO user, final Boolean rememberMe) {
        String authorities = user.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.joining(","));

        long now = (new Date()).getTime();
        Date validity;
        if (rememberMe) {
            validity = new Date(now + this.tokenValidityInMillisecondsForRememberMe);
        } else {
            validity = new Date(now + this.tokenValidityInMilliseconds);
        }

        return Jwts
                .builder()
                .setSubject(user.getUsername())
                .claim(USER_ID_KEY, user.getUserid())
                .claim(USER_NAME_KEY, user.getUsername())
                .claim(USER_EMAIL_KEY, user.getEmail())
                .claim(USER_DOMAIN_ACCOUNT_KEY, user.getDomainAccount())
                .claim(AUTHORITIES_KEY, authorities)
                .signWith(key, SignatureAlgorithm.HS512)
                .setExpiration(validity)
                .compact();
    }

    /**
     * 根据 token 解析出当前登录用户.
     *
     * @param token token
     * @return 当前登录用户
     */
    public LoginUserDTO obtainLoginUserFromToken(final String token) {
        Claims claims = jwtParser.parseClaimsJws(token).getBody();
        Set<String> authorities = Arrays
                .stream(claims.get(AUTHORITIES_KEY).toString().split(","))
                .filter(auth -> !auth.trim().isEmpty())
                .collect(Collectors.toSet());

        return new LoginUserDTO(
                Long.valueOf(String.valueOf(claims.get(USER_ID_KEY))),
                String.valueOf(claims.get(USER_EMAIL_KEY)),
                String.valueOf(claims.get(USER_DOMAIN_ACCOUNT_KEY)),
                String.valueOf(claims.get(USER_NAME_KEY)),
                SystemConstant.EMPTY_STRING,
                authorities
        );
    }

    /**
     * 校验 token.
     *
     * @param token token
     * @return boolean
     */
    public boolean validateToken(final String token) {
        try {
            jwtParser.parseClaimsJws(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            log.info("Invalid jwt token.");
            log.trace("Invalid jwt token trace.", e);
        }
        return false;
    }

    /**
     * 从 Request header 中 获取认证 token.
     *
     * @param request request
     * @return token string
     */
    public String resolveToken(final HttpServletRequest request) {
        String bearerToken = request.getHeader(AUTHORIZATION_HEADER);
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);
        }
        return null;
    }
}
