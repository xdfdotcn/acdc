package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class RdbInstanceServiceIT {

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private RdbService rdbService;

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        RdbInstanceDO rdbInstance = new RdbInstanceDO();
        rdbInstanceService.save(rdbInstance);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveShouldFailWhenGivenNull() {
        rdbInstanceService.save(null);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<RdbInstanceDO> rdbInstanceList = createRdbInstanceList();
        List<RdbInstanceDO> saveResultList = rdbInstanceService.saveAll(rdbInstanceList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(rdbInstanceList.size());

        for (int i = 0; i < rdbInstanceList.size(); i++) {
            rdbInstanceList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(rdbInstanceList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenExist() {
        List<RdbInstanceDO> rdbInstanceList = createRdbInstanceList();
        List<RdbInstanceDO> saveResultList = rdbInstanceService.saveAll(rdbInstanceList);
        saveResultList.forEach(instance -> instance.setVip("192.119.119.119"));
        rdbInstanceService.saveAll(saveResultList).forEach(instance -> {
            Assertions.assertThat(instance.getVip()).isEqualTo("192.119.119.119");
        });
        Assertions.assertThat(rdbInstanceService.saveAll(saveResultList).size()).isEqualTo(rdbInstanceList.size());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveAllShouldFailWhenMissingNotNullField() {
        List<RdbInstanceDO> rdbDatabaseList = createRdbInstanceList();
        rdbDatabaseList.forEach(instance -> instance.setPort(null));
        rdbInstanceService.saveAll(rdbDatabaseList);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveAllShouldFailWhenGivenNull() {
        rdbInstanceService.saveAll(null);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<RdbInstanceDO> rdbInstanceList = createRdbInstanceListByCount(20);
        rdbInstanceService.saveAll(rdbInstanceList);
        RdbInstanceDO rdbInstance = new RdbInstanceDO();
        rdbInstance.setHost("189");
        List<RdbInstanceDO> queryResultList = rdbInstanceService.queryAll(rdbInstance);
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getHost()).contains("189"));

        queryResultList = rdbInstanceService.queryAll(new RdbInstanceDO());
        long p1Count = queryResultList.stream().filter(item -> item.getHost().contains("189")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getHost().contains("188")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryAllShouldFailWhenGiveNull() {
        rdbInstanceService.queryAll(null);
    }

    @Test
    public void testQuery() {
        List<RdbInstanceDO> rdbInstanceList = createRdbInstanceList();
        rdbInstanceService.saveAll(rdbInstanceList);

        // 分页正常滚动
        RdbInstanceDO rdbInstance = new RdbInstanceDO();
        rdbInstance.setHost("168");
        Page<RdbInstanceDO> page = rdbInstanceService.query(rdbInstance, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbInstanceService.query(rdbInstance, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbInstanceService.query(rdbInstance, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbInstanceService.query(rdbInstance, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        rdbInstance.setHost("kk-not-exist");
        page = rdbInstanceService.query(rdbInstance, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = rdbInstanceService.query(new RdbInstanceDO(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        rdbInstance.setHost("168");
        page = rdbInstanceService.query(rdbInstance, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        rdbInstanceService.query(new RdbInstanceDO(), createPageRequest(-999, -666));
    }

    @Test
    public void testCascade() {
        RdbDO rdb = new RdbDO();
        rdb.setId(1L);
        rdb.setRdbType("mysql");
        rdb.setName("cdc-cluster");
        rdb.setUsername("fake-user");
        rdb.setPassword("fake-password");
        RdbDO rdbSaveResult = rdbService.save(rdb);

        RdbDO findRdb = rdbService.findById(rdbSaveResult.getId()).orElseThrow(NotFoundException::new);
        RdbInstanceDO rdbInstance = new RdbInstanceDO();
        rdbInstance.setPort(8080);
        rdbInstance.setHost("192.168.119.0");
        rdbInstance.setRdb(findRdb);
        RdbInstanceDO rdbInstanceSaveResult = rdbInstanceService.save(rdbInstance);

        RdbInstanceDO findRdbInstance = rdbInstanceService.findById(rdbInstanceSaveResult.getId()).orElseThrow(NotFoundException::new);

        Assertions.assertThat(findRdbInstance.getRdb()).isEqualTo(findRdb);
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("host")));
        return pageable;
    }

    private List<RdbInstanceDO> createRdbInstanceListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<RdbInstanceDO> rdbInstanceList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String host = i % 2 == 0 ? "189.110.92." + i : "188.110.92." + i;
            RdbInstanceDO rdbInstance = new RdbInstanceDO();
            rdbInstance.setHost(host);
            rdbInstance.setPort(8080);
            rdbInstance.setVip("210.121.11.1");
            rdbInstance.setCreationTime(new Date().toInstant());
            rdbInstance.setUpdateTime(new Date().toInstant());
            rdbInstanceList.add(rdbInstance);
        }
        return rdbInstanceList;
    }

    private List<RdbInstanceDO> createRdbInstanceList() {
        List<RdbInstanceDO> rdbInstanceList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            RdbInstanceDO rdbInstance = new RdbInstanceDO();
            rdbInstance.setHost("192.168.1." + i);
            rdbInstance.setPort(8080);
            rdbInstance.setVip("210.121.11.1");
            rdbInstance.setCreationTime(new Date().toInstant());
            rdbInstance.setUpdateTime(new Date().toInstant());
            rdbInstanceList.add(rdbInstance);
        }
        return rdbInstanceList;
    }
}
