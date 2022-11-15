package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class UserLoginServiceIT {

    // len 60
    public static final String PASSWD_HASH = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

    @Autowired
    private UserService userService;

    @Autowired
    private AuthorityService authorityService;

    @Autowired
    private UserRepository userRepository;

    @Before
    public void init() {
        userRepository.deleteAll();
    }

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        UserDO user = new UserDO();
        user.setEmail("test@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        user.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(user);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        UserDO user = new UserDO();
        user.setEmail("test@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        user.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(user);

        userService.save(saveResult);
        Assertions.assertThat(userService.queryAll(new UserDO()).size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        UserDO user = new UserDO();
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        user.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(user);
        userService.save(user);
    }

    @Test(expected = DataIntegrityViolationException.class)
    public void testSaveShouldThrowExceptionWithUniqueKeyDuplicate() {
        UserDO user = new UserDO();
        user.setEmail("xxx666@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        user.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(user);

        // duplicate

        UserDO user2 = new UserDO();
        user2.setEmail("xxx666@xdf.cn");
        user2.setPassword(PASSWD_HASH);
        user2.setCreatedBy("admin-test");
        user2.setId(null);
        userService.save(user2);
    }

    @Test
    public void testFindUserByEmailShouldSuccess() {
        UserDO user = new UserDO();
        user.setEmail("xxx666@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        userService.save(user);
        UserDO findUser = userService.findUserByEmail("xxx666@xdf.cn").orElseThrow(NotFoundException::new);
        user.setId(findUser.getId());
        Assertions.assertThat(findUser).isEqualTo(user);
        Assertions.assertThat(userService.findUserByEmail("xxx666").isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindUserByEmailShouldThrowExceptionWithGivenNull() {
        Assertions.assertThat(userService.findUserByEmail(null).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindUserByIdShouldSuccess() {
        UserDO user = new UserDO();
        user.setEmail("xxx666@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        UserDO findUser = userService.findUserById(saveResult.getId()).orElseThrow(NotFoundException::new);
        user.setId(findUser.getId());
        Assertions.assertThat(findUser).isEqualTo(user);
        Assertions.assertThat(userService.findUserById(2L).isPresent()).isEqualTo(false);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testFindUserByIdShouldThrowExceptionWithGivenNull() {
        userService.findUserById(null);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveShouldFailWhenGivenNull() {
        userService.save(null);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<UserDO> userList = createUserList();
        List<UserDO> saveResult = userService.saveAll(userList);

        Assertions.assertThat(saveResult.size()).isEqualTo(userList.size());

        for (int i = 0; i < userList.size(); i++) {
            userList.get(i).setId(saveResult.get(i).getId());
            Assertions.assertThat(saveResult.get(i)).isEqualTo(userList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenExist() {
        List<UserDO> userList = createUserList();
        List<UserDO> saveResult = userService.saveAll(userList);
        saveResult.forEach(u -> u.setName("test_update"));
        userService.saveAll(saveResult).forEach(u -> {
            Assertions.assertThat(u.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(userService.saveAll(saveResult).size()).isEqualTo(userList.size());
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveAllShouldFailWhenGivenNull() {
        userService.saveAll(null);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<UserDO> userList = createUserListByCount(20);
        userService.saveAll(userList);
        UserDO user = new UserDO();
        user.setEmail("test1");
        List<UserDO> queryResult = userService.queryAll(user);
        Assertions.assertThat(queryResult.size()).isEqualTo(10);
        queryResult.forEach(u -> Assertions.assertThat(u.getEmail()).contains("test1"));

        queryResult = userService.queryAll(new UserDO());
        long p1Count = queryResult.stream().filter(item -> item.getEmail().contains("test1")).count();
        long p2Count = queryResult.stream().filter(item -> item.getEmail().contains("test2")).count();
        Assertions.assertThat(queryResult.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test
    public void testQueryAllShouldFailWhenGiveNull() {
        Assertions.assertThat(userService.queryAll(null).isEmpty()).isEqualTo(true);
    }

    @Test
    public void testQuery() {
        List<UserDO> userList = createUserList();
        userService.saveAll(userList);

        // 分页正常滚动
        UserDO user = new UserDO();
        user.setEmail("test");
        Page<UserDO> page = userService.query(user, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = userService.query(user, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = userService.query(user, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = userService.query(user, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        user.setEmail("kk-not-exist");
        page = userService.query(user, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = userService.query(new UserDO(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        user.setEmail("t");
        page = userService.query(user, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        userService.query(new UserDO(), createPageRequest(-999, -666));
    }

    @Test(expected = NullPointerException.class)
    public void testQueryShouldFailWhenGivenNull() {
        Assertions.assertThat(userService.query(null, createPageRequest(1, 2)).getContent().isEmpty()).isEqualTo(true);
        Assertions.assertThat(userService.query(new UserDO(), null));
    }

    @Test
    public void testCascade() {
        AuthorityDO authority1 = authorityService.findByName("ROLE_ADMIN").orElseThrow(NotFoundException::new);
        AuthorityDO authority2 = authorityService.findByName("ROLE_USER").orElseThrow(NotFoundException::new);
        Set<AuthorityDO> authoritySet = new HashSet<>();
        authoritySet.add(authority1);
        authoritySet.add(authority2);
        UserDO user = new UserDO();
        user.setEmail("test@xdf.cn");
        user.setCreatedBy("admin@xdf.cn");
        user.setPassword(PASSWD_HASH);
        user.setAuthorities(authoritySet);
        Long id = userService.save(user).getId();
        UserDO findUser = userService.findUserById(id).orElseThrow(NotFoundException::new);
        Assertions.assertThat(findUser.getAuthorities().size()).isEqualTo(2);
        long adminCount = findUser.getAuthorities()
                .stream()
                .filter(authority -> authority.getName().contains("ADMIN"))
                .count();
        long userCount = findUser.getAuthorities()
                .stream()
                .filter(authority -> authority.getName().contains("ADMIN"))
                .count();
        Assertions.assertThat(adminCount).isEqualTo(1);
        Assertions.assertThat(userCount).isEqualTo(1);
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("email")));
        return pageable;
    }

    private List<UserDO> createUserListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<UserDO> userList = Lists.newArrayList();
        String emailSuffix = "@xdf.cn";
        for (int i = 0; i < count; i++) {
            String email = i % 2 == 0 ? "test1-" + i + emailSuffix : "test2-" + i + emailSuffix;
            UserDO user = new UserDO();
            user.setEmail(email);
            user.setPassword(PASSWD_HASH);
            user.setCreatedBy("admin-test");
            userList.add(user);
        }
        return userList;
    }

    private List<UserDO> createUserList() {
        List<UserDO> userList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            UserDO user = new UserDO();
            user.setEmail("test+" + i + "@xdf.cn");
            user.setPassword(PASSWD_HASH);
            user.setCreatedBy("admin-test");
            userList.add(user);
        }
        return userList;
    }

    @Test
    public void myTest() {
        List<UserDO> userList = new ArrayList<>();
        Map<String, UserDO> userMap = userList.stream().collect(Collectors.toMap(UserDO::getEmail, user -> user));
        userMap.get("ok");

    }
}
