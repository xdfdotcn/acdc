package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.service.error.NotFoundException;
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
public class HdfsNamenodeServiceIT {

    @Autowired
    private HdfsNamenodeService hdfsNamenodeService;

    @Autowired
    private HdfsService hdfsService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        HdfsNamenodeDO hdfsNamenode = new HdfsNamenodeDO();
        hdfsNamenode.setName("node-1");
        hdfsNamenode.setRpcAddress("192.118.110.123");
        hdfsNamenode.setRpcPort("8080");
        HdfsNamenodeDO saveResult = hdfsNamenodeService.save(hdfsNamenode);
        Assertions.assertThat(saveResult).isEqualTo(hdfsNamenode);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        HdfsNamenodeDO hdfsNamenode = new HdfsNamenodeDO();
        hdfsNamenode.setName("node-1");
        hdfsNamenode.setRpcAddress("192.118.110.123");
        hdfsNamenode.setRpcPort("8080");
        HdfsNamenodeDO saveResult1 = hdfsNamenodeService.save(hdfsNamenode);
        saveResult1.setName("test2");
        HdfsNamenodeDO saveResult2 = hdfsNamenodeService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(hdfsNamenodeService.findAll().size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        HdfsNamenodeDO hdfsNamenode = new HdfsNamenodeDO();
        hdfsNamenodeService.save(hdfsNamenode);
    }

    @Test
    public void testSaveShouldThrowException() {
        Throwable throwable = Assertions.catchThrowable(() -> hdfsNamenodeService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        HdfsNamenodeDO hdfsNamenode = new HdfsNamenodeDO();
        hdfsNamenode.setName("node-1");
        hdfsNamenode.setRpcAddress("192.118.110.123");
        hdfsNamenode.setRpcPort("8080");
        HdfsNamenodeDO saveResult = hdfsNamenodeService.save(hdfsNamenode);
        HdfsNamenodeDO findHdfsNameNode = hdfsNamenodeService.findById(saveResult.getId()).orElseThrow(NotFoundException::new);
        hdfsNamenode.setId(findHdfsNameNode.getId());
        Assertions.assertThat(findHdfsNameNode).isEqualTo(hdfsNamenode);
        Assertions.assertThat(hdfsNamenodeService.findById(11L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowException() {
        Throwable throwable = Assertions.catchThrowable(() -> hdfsNamenodeService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {
        HdfsDO hdfs = new HdfsDO();
        hdfs.setName("test-cluster");
        hdfs.setClientFailoverProxyProvider("com.test.failover.TestProvider");
        HdfsDO hdfsSaveResult = hdfsService.save(hdfs);

        HdfsNamenodeDO hdfsNamenode = new HdfsNamenodeDO();
        hdfsNamenode.setName("node-1");
        hdfsNamenode.setRpcAddress("192.118.110.123");
        hdfsNamenode.setRpcPort("8080");
        hdfsNamenode.setHdfs(hdfsSaveResult);
        hdfsNamenodeService.save(hdfsNamenode);
        Assertions.assertThat(hdfsNamenodeService.findById(1L).get().getHdfs()).isEqualTo(hdfsSaveResult);
    }
}
