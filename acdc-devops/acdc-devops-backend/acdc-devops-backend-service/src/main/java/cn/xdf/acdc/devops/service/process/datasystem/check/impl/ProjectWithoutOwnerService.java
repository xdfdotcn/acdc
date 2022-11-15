package cn.xdf.acdc.devops.service.process.datasystem.check.impl;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.process.datasystem.check.MetadataCheckService;
import io.jsonwebtoken.lang.Collections;
import io.jsonwebtoken.lang.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(0)
public class ProjectWithoutOwnerService implements MetadataCheckService {

    private static final String PROJECT_WITHOUT_OWNER_TITLE = "项目缺少负责人信息(project_id,project_name)";

    @Autowired
    private ProjectService projectService;

    @Override
    public Map<String, List<String>> checkMetadataAndReturnErrorMessage() {
        List<ProjectDO> projects = projectService.queryAll(new ProjectQuery());
        List<String> projectsWithoutOwner = projects.stream()
                .filter(projectDO -> projectDO.getOwner() == null)
                .map(ProjectDO::getSignature)
                .collect(Collectors.toList());

        if (Collections.isEmpty(projectsWithoutOwner)) {
            return new HashMap<>();
        }
        return Maps.of(PROJECT_WITHOUT_OWNER_TITLE, projectsWithoutOwner).build();
    }
}
