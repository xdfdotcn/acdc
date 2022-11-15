package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ProjectServiceImpl implements ProjectService {

    @Autowired
    private ProjectRepository projectRepository;

    @Override
    public ProjectDO save(final ProjectDO project) {
        return projectRepository.save(project);
    }

    @Override
    public List<ProjectDO> saveAll(final List<ProjectDO> projectList) {
        return projectRepository.saveAll(projectList);
    }

    @Override
    public Page<ProjectDO> query(final ProjectQuery projectQuery, final Pageable pageable) {
        return projectRepository.findAll(ProjectService.specificationOf(projectQuery), pageable);
    }

    @Override
    public Optional<ProjectDO> findById(final Long id) {
        return projectRepository.findById(id);
    }

    @Override
    public List<ProjectDO> findAllById(final Iterable<Long> ids) {
        return projectRepository.findAllById(ids);
    }

    @Override
    public List<ProjectDO> queryAll(final ProjectQuery projectQuery) {
        return projectRepository.findAll(ProjectService.specificationOf(projectQuery));
    }
}
