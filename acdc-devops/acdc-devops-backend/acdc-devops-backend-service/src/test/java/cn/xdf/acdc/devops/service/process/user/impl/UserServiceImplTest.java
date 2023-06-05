package cn.xdf.acdc.devops.service.process.user.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.repository.AuthorityRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.user.UserService;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class UserServiceImplTest {
    
    @Autowired
    private UserService userService;
    
    @Autowired
    private ProjectRepository projectRepository;
    
    @Autowired
    private AuthorityRepository authorityRepository;
    
    @Before
    public void setUp() {
        Arrays.stream(AuthorityRoleType.values()).forEach(each -> {
            authorityRepository.save(new AuthorityDO().setName(each.name()));
        });
    }
    
    @Test
    public void testCreate() {
        UserDetailDTO userDetailDTO = getUser("test");
        UserDetailDTO savedUserDetailDTO = userService.create(userDetailDTO);
        Assertions.assertThat(savedUserDetailDTO.getEmail()).isEqualTo("test@test.cn");
        Assertions.assertThat(savedUserDetailDTO.getId()).isGreaterThan(0);
        Assertions.assertThat(savedUserDetailDTO.getAuthoritySet()).containsAll(Sets.newSet(AuthorityRoleType.ROLE_USER, AuthorityRoleType.ROLE_ADMIN));
    }
    
    @Test
    public void testCreateShouldThrowExceptionWhenIllegalParameter() {
        UserDetailDTO userDetailDTO1 = getUser("test");
        userDetailDTO1.setEmail(null);
        Assertions.assertThat(Assertions.catchThrowable(() -> userService.create(userDetailDTO1)))
                .isInstanceOf(ClientErrorException.class);
        
        UserDetailDTO userDetailDTO2 = getUser("test");
        userDetailDTO2.setName(null);
        Assertions.assertThat(Assertions.catchThrowable(() -> userService.create(userDetailDTO2)))
                .isInstanceOf(ClientErrorException.class);
        
        UserDetailDTO userDetailDTO3 = getUser("test");
        userDetailDTO3.setPassword(null);
        Assertions.assertThat(Assertions.catchThrowable(() -> userService.create(userDetailDTO3)))
                .isInstanceOf(ClientErrorException.class);
        
        UserDetailDTO userDetailDTO4 = getUser("test");
        userDetailDTO4.setAuthoritySet(null);
        Assertions.assertThat(Assertions.catchThrowable(() -> userService.create(userDetailDTO4)))
                .isInstanceOf(ClientErrorException.class);
    }
    
    @Test
    public void testUpdateUserNameByEmail() {
    
    }
    
    @Test
    public void testResetPassword() {
    
    }
    
    @Test
    public void testResetRole() {
    
    }
    
    @Test
    public void testPagedQuery() {
    
    }
    
    @Test
    public void testQueryUsersByProjectId() {
        List<UserDO> createdUsers = new ArrayList();
        for (int i = 0; i < 5; i++) {
            createdUsers.add(userService.create(getUser("user_" + i)).toDO());
        }
        
        ProjectDO project = new ProjectDO();
        project.setName("project");
        project.setUsers(new HashSet<>(createdUsers.subList(0, 3)));
        project.setOwner(createdUsers.get(0));
        project.setSource(MetadataSourceType.USER_INPUT);
        projectRepository.save(project);
        
        List<UserDTO> resultUsers = userService.queryUsersByProjectId(project.getId());
        
        // assert
        Assertions.assertThat(resultUsers.size()).isEqualTo(project.getUsers().size());
    }
    
    @Test
    public void testGetDetailByEmail() {
    
    }
    
    @Test
    public void testGetByDomainAccount() {
        UserDetailDTO userDetailDTO = getUser("test");
        UserDetailDTO savedUserDetailDTO = userService.create(userDetailDTO);
        
        UserDTO result = userService.getByDomainAccount(savedUserDetailDTO.getDomainAccount());
        
        Assertions.assertThat(result.getDomainAccount()).isEqualTo(result.getDomainAccount());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByDomainAccountShouldThrowExceptionWhenNotFound() {
        userService.getByDomainAccount("acdc");
    }
    
    @Test
    public void testGetByEmail() {
        UserDetailDTO userDetailDTO = getUser("test");
        UserDetailDTO savedUserDetailDTO = userService.create(userDetailDTO);
        
        UserDTO result = userService.getByEmail(savedUserDetailDTO.getEmail());
        
        Assertions.assertThat(result.getEmail()).isEqualTo(result.getEmail());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByEmailShouldThrowExceptionWhenNotFound() {
        userService.getByEmail("acdc");
    }
    
    @Test
    public void testGetById() {
    
    }
    
    @Test
    public void testDeleteByEmail() {
    
    }
    
    @Test
    public void testIsAdmin() {
    
    }
    
    @Test
    public void testIsDBAShouldReturnTrue() {
        UserDetailDTO userDetailDTO = getUser("test");
        UserDetailDTO savedUserDetailDTO = userService.create(userDetailDTO);
        
        boolean result = userService.isDBA(savedUserDetailDTO.getDomainAccount());
        
        Assertions.assertThat(result).isTrue();
    }
    
    @Test
    public void testIsDBAShouldReturnFalse() {
        UserDetailDTO user = getUser("test");
        userService.create(user);
        
        UserDetailDTO otherUser = new UserDetailDTO()
                .setDomainAccount("test2")
                .setEmail("test2@test.cn")
                .setName("test_name2")
                .setPassword("123")
                .setAuthoritySet(Sets.newSet(AuthorityRoleType.ROLE_USER));
        
        UserDetailDTO savedOtherUser = userService.create(otherUser);
        
        boolean result = userService.isDBA(savedOtherUser.getDomainAccount());
        
        Assertions.assertThat(result).isFalse();
    }
    
    @Test
    public void testIsDBA() {
        UserDetailDTO userDetailDTO = getUser("test");
        UserDetailDTO savedUserDetailDTO = userService.create(userDetailDTO);
        
        boolean result = userService.isDBA(savedUserDetailDTO.getDomainAccount());
        
        Assertions.assertThat(result).isTrue();
    }
    
    @Test
    public void testUpsertOnDomainAccount() {
    
    }
    
    private UserDetailDTO getUser(final String domainAccount) {
        return new UserDetailDTO()
                .setDomainAccount(domainAccount)
                .setEmail(domainAccount + "@test.cn")
                .setName("test_name")
                .setPassword("123")
                .setAuthoritySet(Sets.newSet(AuthorityRoleType.ROLE_USER, AuthorityRoleType.ROLE_ADMIN, AuthorityRoleType.ROLE_DBA));
    }
}
