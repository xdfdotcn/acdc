package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class AuthorityServiceIT {

    @Autowired
    private AuthorityService authorityService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        AuthorityDO authority = new AuthorityDO();
        authority.setName("ROLE_TEST");
        AuthorityDO saveResult = authorityService.save(authority);
        Assertions.assertThat(saveResult).isEqualTo(authority);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        AuthorityDO authority = new AuthorityDO();
        authority.setName("ROLE_TEST");
        AuthorityDO saveResult = authorityService.save(authority);
        Assertions.assertThat(saveResult).isEqualTo(authority);

        authorityService.save(saveResult);
        Assertions.assertThat(authorityService.findAll().size()).isEqualTo(3);
    }

    @Test(expected = JpaSystemException.class)
    public void testSaveShouldFailWhenMissingPkField() {
        AuthorityDO authority = new AuthorityDO();
        authority.setName(null);
        authorityService.save(authority);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSaveShouldFailWhenGivenNull() {
        authorityService.save(null);
    }

    @Test
    public void testFindByName() {
        AuthorityDO authority = authorityService.findByName("ROLE_ADMIN").orElseThrow(NotFoundException::new);
        Assertions.assertThat(authority.getName()).isEqualTo("ROLE_ADMIN");
        Assertions.assertThat(authorityService.findByName("test123").isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindAll() {
        Assertions.assertThat(authorityService.findAll().size()).isEqualTo(2);
    }
}
