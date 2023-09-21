package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.QueryScope;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.util.UserUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("api/v1")
public class DataSystemResourceController {

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;

    /**
     * Get data system resource detail by id.
     *
     * @param id data system resource id
     * @return data system resource detail DTO
     */
    @GetMapping("/data-system-resources/{id}")
    public DataSystemResourceDetailDTO getById(@PathVariable("id") final Long id) {
        return dataSystemResourceService.getDetailById(id);
    }

    /**
     * Paged query data system resource detail by id.
     *
     * @param query query
     * @return data system resource detail DTO paged list
     */
    @GetMapping("/data-system-resources")
    public PageDTO<DataSystemResourceDetailDTO> pagedQuery(final DataSystemResourceQuery query) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        boolean isAdmin = UserUtil.isAdmin(currentUser);
        if (query.getScope() == QueryScope.CURRENT_USER && !isAdmin) {
            query.setMemberDomainAccount(currentUser.getDomainAccount());
        }
        Page<DataSystemResourceDetailDTO> page = dataSystemResourceService.pagedQueryDetail(query);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * Create data system resource.
     *
     * @param dataSystemResourceDetailDTO data system resource detail DTO
     */
    @PostMapping("/data-system-resources")
    public void create(@RequestBody final DataSystemResourceDetailDTO dataSystemResourceDetailDTO) {
        dataSystemResourceService.create(dataSystemResourceDetailDTO);
    }

    /**
     * Refresh data system meta data.
     *
     * @param id data system resource id
     */
    @PostMapping("/data-system-resources/{id}/refresh")
    public void refreshDynamicDataSystemResource(@PathVariable("id") final Long id) {
        dataSystemResourceService.refreshDynamicDataSystemResource(id);
    }

    /**
     * Update data system resource.
     *
     * @param dataSystemResourceDetailDTO data system resource detail DTO
     */
    @PutMapping("/data-system-resources/{id}")
    public void update(@RequestBody final DataSystemResourceDetailDTO dataSystemResourceDetailDTO) {
        dataSystemResourceService.update(dataSystemResourceDetailDTO);
    }

    /**
     * Delete data system resource.
     *
     * @param id data system resource id
     */
    @DeleteMapping("/data-system-resources/{id}")
    public void deteleById(@PathVariable("id") final Long id) {
        dataSystemResourceService.deleteById(id);
    }

    /**
     * Create child data system resource.
     *
     * @param parentId                     parent data system resource id
     * @param dataSystemResourceDetailDTOs data system resource detail DTO
     */

    @PostMapping("/data-system-resources/{id}/data-system-resources")
    public void createChildren(@PathVariable("id") final Long parentId,
            @RequestBody final List<DataSystemResourceDetailDTO> dataSystemResourceDetailDTOs) {
        if (dataSystemResourceDetailDTOs.isEmpty()) {
            return;
        }

        // check if every resource has same resource type
        DataSystemResourceType dataSystemResourceType = dataSystemResourceDetailDTOs.get(0).getResourceType();
        dataSystemResourceDetailDTOs.forEach(each -> {
            if (!each.getResourceType().equals(dataSystemResourceType)) {
                throw new ClientErrorException("every resource must have same resource type");
            }
        });

        // use parent resource's project relation
        DataSystemResourceDetailDTO parentResourceDTO = dataSystemResourceService.getDetailById(parentId);
        dataSystemResourceDetailDTOs.forEach(each -> each.setProjects(new ArrayList<>(parentResourceDTO.getProjects())));

        dataSystemResourceService.mergeAllChildrenByName(dataSystemResourceDetailDTOs, dataSystemResourceType, parentId);
    }

    /**
     * Get data collection definition.
     *
     * @param dataCollectionId data collection id
     * @return data collection definition
     */
    @GetMapping("/data-system-resources/{dataCollectionId}/data-collection-definition")
    public DataCollectionDefinition getDataCollectionDefinition(@PathVariable("dataCollectionId") final Long dataCollectionId) {
        DataSystemType dataSystemType = dataSystemResourceService.getDataSystemType(dataCollectionId);

        return dataSystemServiceManager.getDataSystemMetadataService(dataSystemType)
                .getDataCollectionDefinition(dataCollectionId);
    }

    /**
     * Get data system resource definitions.
     *
     * @return data system resource definition list
     */
    @GetMapping("/data-system-resources/definitions")
    public List<DataSystemResourceDefinition> getDataSystemResourceDefinitions() {
        List<DataSystemResourceDefinition> dataSystemResourceDefinitions = new ArrayList<>();

        for (DataSystemType each : DataSystemType.values()) {
            try {
                dataSystemResourceDefinitions.add(dataSystemServiceManager.getDataSystemMetadataService(each).getDataSystemResourceDefinition());
            } catch (ServerErrorException e) {
                log.warn(String.format("ignore error when getting data system definition for %s", each), e);
            }
        }

        return dataSystemResourceDefinitions;
    }
}
