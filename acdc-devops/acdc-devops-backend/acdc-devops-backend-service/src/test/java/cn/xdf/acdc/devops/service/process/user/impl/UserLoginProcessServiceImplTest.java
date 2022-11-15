package cn.xdf.acdc.devops.service.process.user.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.service.entity.UserService;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

// CHECKSTYLE:OFF

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class UserLoginProcessServiceImplTest {

    // len 60
    public static final String PASSWD_HASH = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    @Autowired
    private UserProcessService userProcessService;
    @Autowired
    private UserService userService;

    @Test
    public void testGetUser() {
        UserDTO user = userProcessService.getUser(2L);
        System.out.println(user);
    }

    @Test
    public void testListUser() {
        UserDO user = new UserDO();
        user.setEmail("test@abc.cn");
        user.setPassword(PASSWD_HASH);
        user.setCreatedBy("admin-test");
        UserDO saveResult = userService.save(user);
        user.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(user);

        userService.save(saveResult);
        Assertions.assertThat(userService.queryAll(new UserDO()).size()).isEqualTo(1);

        UserDO user1 = new UserDO();
        user1.setEmail("tony@abc.cn");
        user1.setPassword(PASSWD_HASH);
        user1.setCreatedBy("admin-test");
        UserDO saveResult1 = userService.save(user1);
        user1.setId(saveResult1.getId());
        Assertions.assertThat(saveResult1).isEqualTo(user1);

        List<UserDTO> userDTOList = userProcessService.listUser("tony");
        Assertions.assertThat(userDTOList.size()).isGreaterThan(0);
    }

}
