package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class HdfsServiceIT {

    @Autowired
    private HdfsService hdfsService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        HdfsDO hdfs = new HdfsDO();
        hdfs.setName("test-cluster");
        hdfs.setClientFailoverProxyProvider("com.test.failover.TestProvider");
        HdfsDO saveResult = hdfsService.save(hdfs);
        Assertions.assertThat(saveResult).isEqualTo(hdfs);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        HdfsDO hdfs = new HdfsDO();
        hdfs.setName("test-cluster");
        hdfs.setClientFailoverProxyProvider("com.test.failover.TestProvider");
        HdfsDO saveResult1 = hdfsService.save(hdfs);
        saveResult1.setName("test2");
        HdfsDO saveResult2 = hdfsService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(hdfsService.findAll().size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        HdfsDO hdfs = new HdfsDO();
        hdfsService.save(hdfs);
    }

    @Test
    public void testSaveShouldFailWhenGivenNull() {
        Throwable throwable = Assertions.catchThrowable(() -> hdfsService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }
}
