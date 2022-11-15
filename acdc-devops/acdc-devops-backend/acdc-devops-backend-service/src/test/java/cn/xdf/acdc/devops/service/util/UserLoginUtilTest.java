package cn.xdf.acdc.devops.service.util;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class UserLoginUtilTest {

    @Test
    public void testConvertDomainAccountToEmail() {
        String domainAccount = "user";
        String email = UserUtil.convertDomainAccountToEmail(domainAccount);

        Assertions.assertThat(email).isEqualTo(domainAccount + UserUtil.EMAIL_SUFFIX);
    }

    @Test
    public void testConvertEmailToDomainAccount() {
        String domainAccount = "user";
        String email = domainAccount + UserUtil.EMAIL_SUFFIX;
        String restoredDomainAccount = UserUtil.convertEmailToDomainAccount(email);

        Assertions.assertThat(restoredDomainAccount).isEqualTo(domainAccount);
    }
}
