package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.entity.HiveTableService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.util.BizAssert;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractDatasetProcessService implements DatasetProcessService {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private ProjectRepository projectRepository;

    protected List<ProjectDO> getProjects(final Set<Long> ids) {
        List<ProjectDO> projects = projectService.findAllById(ids);
        BizAssert.notFound(projects.size() == ids.size(), String.format("ids: %s", ids));
        return projects;
    }

    protected List<RdbTableDO> getRdbTables(final Set<Long> ids) {
        List<RdbTableDO> rdbTables = rdbTableService.findAllById(ids);
        BizAssert.notFound(rdbTables.size() == ids.size(), String.format("ids: %s", ids));
        return rdbTables;
    }

    protected List<HiveTableDO> getHiveTables(final Set<Long> ids) {
        List<HiveTableDO> hiveTables = hiveTableService.findAllById(ids);
        BizAssert.notFound(hiveTables.size() == ids.size(), String.format("ids: %s", ids));
        return hiveTables;
    }

    protected List<KafkaTopicDO> getKafkaTopics(final Set<Long> ids) {
        List<KafkaTopicDO> kafkaTopics = kafkaTopicService.findAllById(ids);
        BizAssert.notFound(kafkaTopics.size() == ids.size(), String.format("ids: %s", ids));
        return kafkaTopics;
    }

    protected List<RdbInstanceDO> getRdbInstances(final Set<Long> ids) {
        List<RdbInstanceDO> rdbInstances = rdbInstanceService.findAllById(ids);
        BizAssert.notFound(rdbInstances.size() == ids.size(), String.format("ids: %s", ids));
        return rdbInstances;
    }

    protected void verifyProject(final List<DataSetDTO> datasets) {
        Set<Long> ids = datasets.stream().map(DataSetDTO::getProjectId).collect(Collectors.toSet());
        getProjects(ids);
    }

    protected ProjectDTO getProject(final Long projectId) {
        return projectRepository.findById(projectId)
            .map(ProjectDTO::new)
            .orElseThrow(() -> new NotFoundException(String.format("projectId: %s", projectId)));
    }
}
