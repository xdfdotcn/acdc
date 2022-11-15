package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
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

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorServiceIT {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private UserService userService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private ConnectClusterService connectClusterService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName("test");
        ConnectorDO connectorSaveResult = connectorService.save(connector);
        Assertions.assertThat(connectorSaveResult).isEqualTo(connector);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName("test");
        ConnectorDO saveResult1 = connectorService.save(connector);
        saveResult1.setName("test2");
        ConnectorDO saveResult2 = connectorService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(connectorService.query(new ConnectorQuery()).size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName(null);
        Throwable throwable = Assertions.catchThrowable(() -> connectorService.save(connector));
        Assertions.assertThat(throwable).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<ConnectorDO> connectorList = createConnectorList();
        List<ConnectorDO> saveResultList = connectorService.saveAll(connectorList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(connectorList.size());

        for (int i = 0; i < connectorList.size(); i++) {
            connectorList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(connectorList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenAlreadyExist() {
        List<ConnectorDO> connectorList = createConnectorList();
        List<ConnectorDO> saveResultList = connectorService.saveAll(connectorList);
        saveResultList.forEach(table -> table.setName("test_update"));
        connectorService.saveAll(saveResultList).forEach(table -> {
            Assertions.assertThat(table.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(connectorService.saveAll(saveResultList).size()).isEqualTo(connectorList.size());
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyCollection() {
        connectorService.saveAll(Lists.newArrayList());
        Assertions.assertThat(connectorService.query(new ConnectorQuery()).size()).isEqualTo(0);
    }

    @Test
    public void testSaveAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorService.saveAll(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<ConnectorDO> connectorList = createConnectorListByCount(20);
        connectorService.saveAll(connectorList);
        ConnectorQuery connectorQuery = new ConnectorQuery();
        connectorQuery.setName("connector1");
        List<ConnectorDO> queryResultList = connectorService.query(connectorQuery);
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getName()).contains("connector1"));

        queryResultList = connectorService.query(new ConnectorQuery());
        long p1Count = queryResultList.stream().filter(item -> item.getName().contains("connector1")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getName().contains("connector2")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test
    public void testQueryAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorService.query(null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testQueryShouldSuccess() {
        List<ConnectorDO> connectorList = createConnectorList();
        connectorService.saveAll(connectorList);

        // 分页正常滚动
        ConnectorQuery connectorQuery = new ConnectorQuery();
        connectorQuery.setName("connector");
        Page<ConnectorDO> page = connectorService.pageQuery(connectorQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = connectorService.pageQuery(connectorQuery, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = connectorService.pageQuery(connectorQuery, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = connectorService.pageQuery(connectorQuery, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        connectorQuery.setName("kk-not-exist");
        page = connectorService.pageQuery(connectorQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = connectorService.pageQuery(new ConnectorQuery(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        connectorQuery.setName("connector");
        page = connectorService.pageQuery(connectorQuery, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test
    public void testQueryShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorService.pageQuery(null, null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> connectorService.pageQuery(new ConnectorQuery(), null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> connectorService.pageQuery(new ConnectorQuery(), createPageRequest(-999, -666)));
        Assertions.assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName("test");
        ConnectorDO connectorSaveResult = connectorService.save(connector);
        Assertions.assertThat(connectorService.findById(connectorSaveResult.getId()).isPresent()).isEqualTo(true);
        Assertions.assertThat(connectorService.findById(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<ConnectorDO> createConnectorListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<ConnectorDO> connectorList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "connector1-" + i : "connector2-" + i;
            ConnectorDO connector = new ConnectorDO();
            connector.setName(name);
            connectorList.add(connector);
        }
        return connectorList;
    }

    private List<ConnectorDO> createConnectorList() {
        List<ConnectorDO> connectorList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            ConnectorDO connector = new ConnectorDO();
            connector.setName("connector" + i);
            connectorList.add(connector);
        }
        return connectorList;
    }
}
