package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.query.HiveQuery;
import cn.xdf.acdc.devops.repository.HiveRepository;
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
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class HiveServiceIT {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private HdfsService hdfsService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private HiveRepository hiveRepository;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        HiveDO hive = new HiveDO();
        hive.setName("test");
        hive.setMetastoreUris("test://test/test");
        hive.setHdfsUser("hive");
        hiveService.save(hive);
        HiveDO saveResult = hiveService.save(hive);

        hive.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(hive);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        HiveDO hive = new HiveDO();
        hive.setName("test");
        hive.setMetastoreUris("test://test/test");
        hive.setHdfsUser("hive");
        hiveService.save(hive);
        HiveDO saveResult1 = hiveService.save(hive);
        saveResult1.setName("test2");
        HiveDO saveResult2 = hiveService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(hiveRepository.queryAll(new HiveQuery()).size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        HiveDO hive = new HiveDO();
        hiveService.save(hive);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveShouldFailWhenGivenNull() {
        hiveService.save(null);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<HiveDO> hiveList = createHiveListByCount(20);
        hiveService.saveAll(hiveList);

        HiveQuery query = HiveQuery.builder()
                .name("h1")
                .build();

        List<HiveDO> queryResult = hiveRepository.queryAll(query);
        Assertions.assertThat(queryResult.size()).isEqualTo(10);
        queryResult.forEach(h -> Assertions.assertThat(h.getName()).contains("h1"));

        queryResult = hiveRepository.queryAll(new HiveQuery());
        long p1Count = queryResult.stream().filter(item -> item.getName().contains("h1")).count();
        long p2Count = queryResult.stream().filter(item -> item.getName().contains("h2")).count();
        Assertions.assertThat(queryResult.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test(expected = NullPointerException.class)
    public void testQueryAllShouldFailWhenGiveNull() {
        hiveRepository.queryAll(null);
    }

    @Test
    public void testQuery() {
        List<HiveDO> hiveList = createHiveList();
        hiveService.saveAll(hiveList);

        // 分页正常滚动
        HiveQuery query = HiveQuery.builder()
                .name("test")
                .build();

        Page<HiveDO> page = hiveRepository.queryAll(query, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = hiveRepository.queryAll(query, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = hiveRepository.queryAll(query, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = hiveRepository.queryAll(query, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        query.setName("kk-not-exist");
        page = hiveRepository.queryAll(query, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = hiveRepository.queryAll(new HiveQuery(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        query.setName("test");
        page = hiveRepository.queryAll(query, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        hiveRepository.queryAll(new HiveQuery(), createPageRequest(-999, -666));
    }

    @Test
    public void testCascade() {
        // hdfs
        HdfsDO hdfs = new HdfsDO();
        hdfs.setName("test-cluster");
        hdfs.setClientFailoverProxyProvider("com.test.failover.TestProvider");
        HdfsDO hdfsSaveResult = hdfsService.save(hdfs);

        // project
        List<ProjectDO> projectList = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            ProjectDO project = new ProjectDO();
            project.setId(Long.valueOf(i + 1));
            project.setName("project-test");
            project.setDescription("project-desc");
            project.setUpdateTime(new Date().toInstant());
            project.setCreationTime(new Date().toInstant());
            projectList.add(project);
        }
        List<ProjectDO> projectSaveResult = projectService.saveAll(projectList);

        HiveDO hive = new HiveDO();
        hive.setName("test");
        hive.setMetastoreUris("test://test/test");
        hive.setHdfsUser("hive");
        hive.setHdfs(hdfsSaveResult);
        hive.setProjects(projectSaveResult.stream().collect(Collectors.toSet()));
        HiveDO saveResult = hiveService.save(hive);
        Assertions.assertThat(hiveService.findById(saveResult.getId()).get().getHdfs()).isEqualTo(hdfsSaveResult);

        List<Long> findPrjIdList = saveResult.getProjects().stream()
                .map(prj -> prj.getId()).collect(Collectors.toList());

        List<Long> prjIdList = projectSaveResult.stream()
                .map(prj -> prj.getId()).collect(Collectors.toList());

        Assertions.assertThat(findPrjIdList.size()).isEqualTo(prjIdList.size());
        Assertions.assertThat(findPrjIdList.stream().sorted().collect(Collectors.toList()))
                .isEqualTo(prjIdList.stream().sorted().collect(Collectors.toList()));
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<HiveDO> createHiveListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<HiveDO> hiveList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "h1-" + i : "h2-" + i;
            HiveDO hive = new HiveDO();
            hive.setName(name);
            hive.setMetastoreUris("//:test");
            hive.setHdfsUser("hive");
            hiveList.add(hive);
        }
        return hiveList;
    }

    private List<HiveDO> createHiveList() {
        List<HiveDO> hiveList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            HiveDO hive = new HiveDO();
            hive.setName("test" + i);
            hive.setMetastoreUris("test://test/test");
            hive.setHdfsUser("hive");
            hiveList.add(hive);
        }
        return hiveList;
    }
}
