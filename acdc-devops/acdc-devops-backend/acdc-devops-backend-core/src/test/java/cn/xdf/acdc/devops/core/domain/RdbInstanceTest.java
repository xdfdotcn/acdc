package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class RdbInstanceTest {

    @Test
    public void testEquals() {
        RdbInstanceDO rdbInstance1 = new RdbInstanceDO();
        rdbInstance1.setRdb(RdbDO.builder().id(1L).build());
        rdbInstance1.setRole(RoleType.MASTER);
        rdbInstance1.setHost("10.211.55.2");
        rdbInstance1.setPort(3306);

        RdbInstanceDO rdbInstance2 = new RdbInstanceDO();
        rdbInstance2.setRdb(RdbDO.builder().id(1L).build());
        rdbInstance2.setRole(RoleType.MASTER);
        rdbInstance2.setHost("10.211.55.2");
        rdbInstance2.setPort(3306);
        Assertions.assertThat(rdbInstance1.equals(rdbInstance2)).isEqualTo(true);

        rdbInstance2.setRdb(RdbDO.builder().id(1L).build());
        rdbInstance2.setRole(RoleType.MASTER);
        rdbInstance2.setHost("10.211.55.2");
        rdbInstance2.setPort(3306);
        Assertions.assertThat(rdbInstance1.equals(rdbInstance2)).isEqualTo(true);

        rdbInstance2.setRdb(RdbDO.builder().id(1L).build());
        rdbInstance2.setRole(RoleType.MASTER);
        rdbInstance2.setHost("10.211.55.1");
        rdbInstance2.setPort(3306);
        Assertions.assertThat(rdbInstance1.equals(rdbInstance2)).isEqualTo(false);

        rdbInstance2.setRdb(RdbDO.builder().id(1L).build());
        rdbInstance2.setRole(RoleType.MASTER);
        rdbInstance2.setHost("10.211.55.2");
        rdbInstance2.setPort(3307);
        Assertions.assertThat(rdbInstance1.equals(rdbInstance2)).isEqualTo(false);

        rdbInstance2.setRdb(RdbDO.builder().id(2L).build());
        rdbInstance2.setRole(RoleType.MASTER);
        rdbInstance2.setHost("10.211.55.2");
        rdbInstance2.setPort(3306);
        Assertions.assertThat(rdbInstance1.equals(rdbInstance2)).isEqualTo(false);

    }
}
