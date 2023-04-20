package cn.xdf.acdc.devops.service.process.check.impl;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.service.process.check.CheckerInOrder;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import io.jsonwebtoken.lang.Collections;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(0)
public class ProjectWithoutOwner implements CheckerInOrder {

    private static final String PROJECT_WITHOUT_OWNER_TITLE = "项目缺少负责人信息(project_id,project_name)";

    private final ProjectService projectService;

    public ProjectWithoutOwner(final ProjectService projectService) {
        this.projectService = projectService;
    }

    @Override
    public Map<String, List<String>> checkMetadataAndReturnErrorMessage() {
        List<ProjectDTO> projects = projectService.query(new ProjectQuery());
        List<String> projectsWithoutOwner = projects.stream()
                .filter(ProjectDTO -> ProjectDTO.getOwnerId() == null)
                .map(this::getSignature)
                .collect(Collectors.toList());

        if (Collections.isEmpty(projectsWithoutOwner)) {
            return new HashMap<>();
        }
        Map<String, List<String>> result = new LinkedHashMap<>();
        result.put(PROJECT_WITHOUT_OWNER_TITLE, projectsWithoutOwner);
        return result;
    }

    private String getSignature(final ProjectDTO projectDTO) {
        return projectDTO.getId() + "," + projectDTO.getName();
    }
}
