package cn.xdf.acdc.devops.service.process.datasystem.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourceConfigurationRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.DataSystem.Check;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataSystemResourceServiceImpl implements DataSystemResourceService {
    
    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;
    
    @Autowired
    private DataSystemResourceConfigurationRepository dataSystemResourceConfigurationRepository;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Autowired
    private I18nService i18n;
    
    @Transactional
    @Override
    public DataSystemResourceDTO getById(final Long resourceId) {
        return new DataSystemResourceDTO(dataSystemResourceRepository.getOne(resourceId));
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDTO> getByIds(final List<Long> resourceIds) {
        List<DataSystemResourceDTO> result = new ArrayList<>();
        resourceIds.forEach(each -> {
            result.add(getById(each));
        });
        return result;
    }
    
    @Transactional
    @Override
    public DataSystemResourceDetailDTO getDetailById(final Long resourceId) {
        return new DataSystemResourceDetailDTO(dataSystemResourceRepository.getOne(resourceId));
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDTO> getChildren(final Long resourceId, final DataSystemResourceType childrenResourceType) {
        Preconditions.checkArgument(Objects.nonNull(resourceId), "resource id can not be null");
        return dataSystemResourceRepository.findByDeletedFalseAndParentResourceIdAndResourceType(resourceId, childrenResourceType)
                .stream()
                .map(DataSystemResourceDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> getDetailChildren(final Long resourceId, final DataSystemResourceType childrenResourceType) {
        Preconditions.checkState(Objects.nonNull(resourceId), "resource id can not be null");
        return dataSystemResourceRepository.findByDeletedFalseAndParentResourceIdAndResourceType(resourceId, childrenResourceType)
                .stream()
                .map(DataSystemResourceDetailDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> getDetailChildren(
            final Long resourceId,
            final DataSystemResourceType childrenResourceType,
            final String configurationName,
            final String configurationValue) {
        Preconditions.checkState(Objects.nonNull(resourceId), "resource id can not be null");
        
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(resourceId);
        query.setResourceTypes(Arrays.asList(childrenResourceType));
        
        Map<String, String> configurations = new HashMap<>();
        configurations.put(configurationName, configurationValue);
        query.setResourceConfigurations(configurations);
        
        return dataSystemResourceRepository.query(query)
                .stream()
                .map(DataSystemResourceDetailDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public DataSystemType getDataSystemType(final Long resourceId) {
        return dataSystemResourceRepository.getOne(resourceId).getDataSystemType();
    }
    
    @Transactional
    @Override
    public DataSystemResourceDTO getParent(final Long resourceId, final DataSystemResourceType parentResourceType) {
        Optional<DataSystemResourceDO> optional = getParentDO(resourceId, parentResourceType);
        if (optional.isPresent()) {
            return new DataSystemResourceDTO(optional.get());
        }
        throw new EntityNotFoundException(String.format("can not find resource id: %s 's parent with resource type %s", resourceId, parentResourceType));
    }
    
    @Transactional
    @Override
    public DataSystemResourceDetailDTO getDetailParent(final Long resourceId, final DataSystemResourceType parentResourceType) {
        Optional<DataSystemResourceDO> optional = getParentDO(resourceId, parentResourceType);
        if (optional.isPresent()) {
            return new DataSystemResourceDetailDTO(optional.get());
        }
        throw new EntityNotFoundException(String.format("can not find resource id: %s 's parent with resource type %s", resourceId, parentResourceType));
    }
    
    private Optional<DataSystemResourceDO> getParentDO(final Long resourceId, final DataSystemResourceType parentResourceType) {
        DataSystemResourceDO parent = dataSystemResourceRepository.getOne(resourceId).getParentResource();
        
        if (Objects.isNull(parent)) {
            return Optional.empty();
        }
        
        if (parentResourceType.equals(parent.getResourceType())) {
            return Optional.of(parent);
        }
        
        return getParentDO(parent.getId(), parentResourceType);
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDTO> getAllRoots() {
        return dataSystemResourceRepository.findByDeletedFalseAndParentResourceIdIsNull().stream().map(DataSystemResourceDTO::new).collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDTO> query(final DataSystemResourceQuery query) {
        return dataSystemResourceRepository.query(query)
                .stream()
                .map(DataSystemResourceDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> queryDetail(final DataSystemResourceQuery query) {
        return dataSystemResourceRepository.query(query)
                .stream()
                .map(DataSystemResourceDetailDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public Page<DataSystemResourceDTO> pagedQuery(final DataSystemResourceQuery query) {
        return dataSystemResourceRepository.pagedQuery(query).map(DataSystemResourceDTO::new);
    }
    
    @Transactional
    @Override
    public Page<DataSystemResourceDetailDTO> pagedQueryDetail(final DataSystemResourceQuery query) {
        return dataSystemResourceRepository.pagedQuery(query).map(DataSystemResourceDetailDTO::new);
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> mergeAllChildrenByName(
            final List<DataSystemResourceDetailDTO> dataSystemResources,
            final DataSystemResourceType resourceType,
            final Long parentResourceId) {
        List<DataSystemResourceDetailDTO> savedResources = this.getDetailChildren(parentResourceId, resourceType);
        Map<String, DataSystemResourceDetailDTO> nameToSavedResources = savedResources.stream().collect(Collectors.toMap(each -> each.getName(), each -> each));
        Map<String, DataSystemResourceDetailDTO> nameToActualResources = dataSystemResources.stream().collect(Collectors.toMap(each -> each.getName(), each -> each));
        
        // handle to be deleted resources
        deleteResourcesIfNeeded(nameToSavedResources, nameToActualResources);
        
        // handle to be updated resources
        List<DataSystemResourceDetailDTO> toBeUpdateResources = getToBeUpdatedResource(nameToSavedResources, nameToActualResources);
        List<DataSystemResourceDetailDTO> updateResult = this.batchUpdate(toBeUpdateResources);
        for (DataSystemResourceDetailDTO each : updateResult) {
            nameToActualResources.put(each.getName(), each);
        }
        
        // handle to be created resources
        List<DataSystemResourceDetailDTO> toBeCreatedResources = getToBeCreatedResource(nameToSavedResources, nameToActualResources);
        List<DataSystemResourceDetailDTO> createResult = batchCreate(toBeCreatedResources);
        for (DataSystemResourceDetailDTO each : createResult) {
            nameToActualResources.put(each.getName(), each);
        }
        
        return new ArrayList<>(nameToActualResources.values());
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> mergeAllChildrenByNameWithoutCheck(
            final List<DataSystemResourceDetailDTO> dataSystemResources,
            final DataSystemResourceType resourceType,
            final Long parentResourceId) {
        List<DataSystemResourceDetailDTO> savedResources = this.getDetailChildren(parentResourceId, resourceType);
        Map<String, DataSystemResourceDetailDTO> nameToSavedResources = savedResources.stream().collect(Collectors.toMap(each -> each.getName(), each -> each));
        Map<String, DataSystemResourceDetailDTO> nameToActualResources = dataSystemResources.stream().collect(Collectors.toMap(each -> each.getName(), each -> each));
        
        deleteResourcesIfNeeded(nameToSavedResources, nameToActualResources);
        
        List<DataSystemResourceDetailDTO> changedResources = new ArrayList<>();
        changedResources.addAll(getToBeUpdatedResource(nameToSavedResources, nameToActualResources));
        changedResources.addAll(getToBeCreatedResource(nameToSavedResources, nameToActualResources));
        this.createOrUpdateAllWithoutCheck(changedResources);
        
        changedResources.forEach(each -> nameToActualResources.put(each.getName(), each));
        
        return new ArrayList<>(nameToActualResources.values());
    }
    
    private List<DataSystemResourceDetailDTO> deleteResourcesIfNeeded(final Map<String, DataSystemResourceDetailDTO> nameToSavedResource,
                                                                      final Map<String, DataSystemResourceDetailDTO> nameToActualResources) {
        List<DataSystemResourceDetailDTO> deletedResources = new ArrayList<>();
        nameToSavedResource.forEach((name, savedResource) -> {
            // if actual resources do not contains one saved resource's name, this resource should be deleted.
            if (!nameToActualResources.containsKey(name)) {
                this.deleteById(savedResource.getId());
                savedResource.setDeleted(Boolean.TRUE);
                deletedResources.add(savedResource);
            }
        });
        return deletedResources;
    }
    
    private List<DataSystemResourceDetailDTO> getToBeCreatedResource(final Map<String, DataSystemResourceDetailDTO> nameToSavedResource,
                                                                     final Map<String, DataSystemResourceDetailDTO> nameToActualResources) {
        List<DataSystemResourceDetailDTO> toBeInsertedResources = new ArrayList<>();
        nameToActualResources.forEach((name, actualResource) -> {
            if (!nameToSavedResource.containsKey(name)) {
                toBeInsertedResources.add(actualResource);
            }
        });
        return toBeInsertedResources;
    }
    
    private List<DataSystemResourceDetailDTO> getToBeUpdatedResource(final Map<String, DataSystemResourceDetailDTO> nameToSavedResource,
                                                                     final Map<String, DataSystemResourceDetailDTO> nameToActualResources) {
        List<DataSystemResourceDetailDTO> toBeUpdatedResources = new ArrayList<>();
        nameToActualResources.forEach((name, actualResource) -> {
            if (nameToSavedResource.containsKey(name)) {
                DataSystemResourceDetailDTO savedResource = nameToSavedResource.get(name);
                
                if (!Objects.equals(actualResource.getDescription(), savedResource.getDescription())
                        || !Objects.equals(actualResource.getDataSystemResourceConfigurations(), savedResource.getDataSystemResourceConfigurations())
                        || !Objects.equals(actualResource.getProjects(), savedResource.getProjects())) {
                    actualResource.setId(savedResource.getId());
                    toBeUpdatedResources.add(actualResource);
                }
            }
        });
        return toBeUpdatedResources;
    }
    
    @Transactional
    @Override
    public DataSystemResourceDetailDTO create(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        encryptSensitiveConfiguration(dataSystemResourceDetail);
        this.checkDataSystem(dataSystemResourceDetail);
        DataSystemResourceDO dataSystemResourceDO = dataSystemResourceRepository.save(dataSystemResourceDetail.toDO());
        return new DataSystemResourceDetailDTO(dataSystemResourceDO);
    }
    
    private void encryptSensitiveConfiguration(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        // fixme: use real data system definition to check if the configuration is sensitive
        dataSystemResourceDetail.getDataSystemResourceConfigurations().computeIfPresent(Authorization.PASSWORD.getName(), (key, value) -> {
            String originalValue = value.getValue();
            value.setValue(EncryptUtil.encrypt(originalValue));
            return value;
        });
    }
    
    private void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        DataSystemType dataSystemType = dataSystemResourceDetail.getDataSystemType();
        try {
            dataSystemServiceManager.getDataSystemMetadataService(dataSystemType).checkDataSystem(dataSystemResourceDetail);
        } catch (ServerErrorException e) {
            log.warn("error when check data system", e);
            throw new ClientErrorException(i18n.msg(Check.WRONG_ENDPOINT_OR_PASSWORD));
        }
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> batchCreate(final List<DataSystemResourceDetailDTO> dataSystemResourceDetails) {
        List<DataSystemResourceDetailDTO> result = new ArrayList<>();
        dataSystemResourceDetails.forEach(each -> {
            this.create(each);
            result.add(each);
        });
        return result;
    }
    
    @Transactional
    @Override
    public DataSystemResourceDetailDTO update(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        encryptSensitiveConfiguration(dataSystemResourceDetail);
        this.checkDataSystem(dataSystemResourceDetail);
        
        // handle to be delete configurations
        deleteOrMergeConfigurationsIfNeeded(dataSystemResourceDetail);
        
        return new DataSystemResourceDetailDTO(dataSystemResourceRepository.save(dataSystemResourceDetail.toDO()));
    }
    
    private void deleteOrMergeConfigurationsIfNeeded(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        DataSystemResourceDO savedConfigurations = dataSystemResourceRepository.getOne(dataSystemResourceDetail.getId());
        Map<String, DataSystemResourceConfigurationDTO> nameToActualConfigurations = dataSystemResourceDetail.getDataSystemResourceConfigurations();
        savedConfigurations.getDataSystemResourceConfigurations().forEach(each -> {
            // do merge if configuration name exists in actual configurations
            if (nameToActualConfigurations.containsKey(each.getName())) {
                DataSystemResourceConfigurationDTO actualConfiguration = nameToActualConfigurations.get(each.getName());
                actualConfiguration.setId(each.getId());
                actualConfiguration.setCreationTime(each.getCreationTime());
            } else {
                // or do delete
                dataSystemResourceConfigurationRepository.delete(each);
            }
        });
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> batchUpdate(final List<DataSystemResourceDetailDTO> dataSystemResourceDetails) {
        List<DataSystemResourceDetailDTO> result = new ArrayList<>();
        dataSystemResourceDetails.forEach(each -> {
            result.add(update(each));
        });
        return result;
    }
    
    @Transactional
    @Override
    public List<DataSystemResourceDetailDTO> createOrUpdateAllWithoutCheck(final List<DataSystemResourceDetailDTO> dataSystemResourceDetails) {
        List<DataSystemResourceDetailDTO> result = new ArrayList<>();
        dataSystemResourceDetails.forEach(each -> {
            encryptSensitiveConfiguration(each);
            DataSystemResourceDO dataSystemResourceDO = dataSystemResourceRepository.save(each.toDO());
            result.add(new DataSystemResourceDetailDTO(dataSystemResourceDO));
        });
        return result;
    }
    
    @Transactional
    @Override
    public void deleteById(final Long id) {
        DataSystemResourceDO dataSystemResourceDO = dataSystemResourceRepository.getOne(id);
        dataSystemResourceDO.setDeleted(Boolean.TRUE);
        dataSystemResourceRepository.save(dataSystemResourceDO);
        
        dataSystemResourceDO.getChildrenResources().forEach(each -> deleteById(each.getId()));
    }
    
    @Transactional
    @Override
    public void batchDeleteByIds(final Collection<Long> logicalDeleteIds) {
        logicalDeleteIds.forEach(each -> {
            this.deleteById(each);
        });
    }
    
    @Transactional
    @Override
    public void refreshDynamicDataSystemResource(final Long id) {
        dataSystemServiceManager.getDataSystemMetadataService(getDataSystemType(id)).refreshDynamicDataSystemResource(id);
    }
}
