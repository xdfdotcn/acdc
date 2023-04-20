package cn.xdf.acdc.devops.service.process.tool.command;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.service.process.tool.command.ApiUserCommand.CommandEntity.Operation;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.util.UIError;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class ApiUserCommand implements Command<ApiUserCommand.CommandEntity> {

    private final Map<CommandEntity.Operation, Function<CommandEntity, Map<String, Object>>> commandExecutors = new HashMap<>();

    @Autowired
    private I18nService i18n;

    @Autowired
    private UserService userService;

    public ApiUserCommand() {
        commandExecutors.put(Operation.CREATE, this::doCreate);
        commandExecutors.put(Operation.DELETE, this::doDelete);
        commandExecutors.put(Operation.UPDATE, this::doUpdate);
        commandExecutors.put(Operation.GET, this::doGet);
        commandExecutors.put(Operation.LIST, this::doList);
        commandExecutors.put(Operation.RESET_PASSWORD, this::doResetPassword);
        commandExecutors.put(Operation.RESET_ROLE, this::doResetRole);
    }

    @Override
    public Map<String, Object> execute(final CommandEntity entity) {
        return commandExecutors.getOrDefault(entity.opt, this::doNothing).apply(entity);
    }

    private Map<String, Object> doCreate(final CommandEntity entity) {
        Set<AuthorityRoleType> roleTypeSet = CollectionUtils.isEmpty(entity.roles) ? Collections.EMPTY_SET
                : entity.roles.stream().collect(Collectors.toSet());

        UserDetailDTO user = userService.create(
                new UserDetailDTO()
                        .setName(entity.username)
                        .setEmail(entity.email)
                        .setPassword(entity.password)
                        .setAuthoritySet(roleTypeSet)
        );

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", user.getId());
        result.put("email", user.getEmail());
        result.put("name", user.getName());
        result.put("domainAccount", user.getDomainAccount());
        return result;
    }

    private Map<String, Object> doDelete(final CommandEntity entity) {
        userService.deleteByEmail(entity.email);
        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doUpdate(final CommandEntity entity) {
        UserDTO user = userService.updateUserNameByEmail(
                entity.username,
                entity.email
        );
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", user.getId());
        result.put("email", user.getEmail());
        result.put("name", user.getName());
        result.put("domainAccount", user.getDomainAccount());
        return result;
    }

    private Map<String, Object> doGet(final CommandEntity entity) {
        UserDetailDTO user = userService.getDetailByEmail(entity.email);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", user.getId());
        result.put("email", user.getEmail());
        result.put("name", user.getName());
        result.put("domainAccount", user.getDomainAccount());
        result.put("authority", user.getAuthoritySet());
        return result;
    }

    private Map<String, Object> doList(final CommandEntity entity) {
        UserQuery userQuery = new UserQuery();
        userQuery.setCurrent(entity.begin);
        userQuery.setPageSize(entity.pagesize);
        Page<UserDTO> page = userService.pagedQuery(userQuery);

        List<Map<String, Object>> users = page.getContent().stream().map(it -> {
            Map<String, Object> user = new LinkedHashMap<>();
            user.put("email", it.getEmail());
            user.put("name", it.getName());
            return user;
        }).collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("content", users);
        result.put("totalElements", page.getTotalElements());
        return result;
    }

    private Map<String, Object> doResetPassword(final CommandEntity entity) {
        userService.resetPassword(entity.email, entity.oldPassword, entity.newPassword);
        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doResetRole(final CommandEntity entity) {
        Set<AuthorityRoleType> roleTypeSet = CollectionUtils.isEmpty(entity.roles) ? Collections.EMPTY_SET
                : entity.roles.stream().collect(Collectors.toSet());

        userService.resetRole(entity.email, roleTypeSet);
        Map<String, Object> result = new LinkedHashMap<>();
        return result;
    }

    private Map<String, Object> doNothing(final CommandEntity entity) {
        return UIError.getBriefStyleMsg(HttpStatus.BAD_REQUEST, i18n.msg(I18nKey.Command.OPERATION_NOT_SPECIFIED, String.valueOf(entity.opt)));
    }

    // CHECKSTYLE:OFF
    public static class CommandEntity {

        private Operation opt;

        private String username;

        private String email;

        private String password;

        private List<AuthorityRoleType> roles;

        private Integer begin;

        private Integer pagesize;

        private String oldPassword;

        private String newPassword;

        public Operation getOpt() {
            return opt;
        }

        public void setOpt(Operation opt) {
            this.opt = opt;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<AuthorityRoleType> getRoles() {
            return roles;
        }

        public void setRoles(List<AuthorityRoleType> roles) {
            this.roles = roles;
        }

        public Integer getBegin() {
            return begin;
        }

        public void setBegin(Integer begin) {
            this.begin = begin;
        }

        public Integer getPagesize() {
            return pagesize;
        }

        public void setPagesize(Integer pagesize) {
            this.pagesize = pagesize;
        }

        public String getOldPassword() {
            return oldPassword;
        }

        public void setOldPassword(String oldPassword) {
            this.oldPassword = oldPassword;
        }

        public String getNewPassword() {
            return newPassword;
        }

        public void setNewPassword(String newPassword) {
            this.newPassword = newPassword;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("opt:").append(opt).append(" ");
            sb.append("username:").append(username).append(" ");
            sb.append("email:").append(email).append(" ");
            sb.append("password:").append(password).append(" ");
            sb.append("roles:").append(roles).append(" ");
            sb.append("begin:").append(begin).append(" ");
            sb.append("pagesize:").append(pagesize).append(" ");
            sb.append("oldPassword:").append(oldPassword).append(" ");
            sb.append("newPassword:").append(newPassword).append(" ");
            return sb.toString();
        }

        public enum Operation {
            LOGIN, CREATE, DELETE, UPDATE, GET, LIST, RESET_PASSWORD, RESET_ROLE
        }
    }

}
