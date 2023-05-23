package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import org.springframework.data.domain.Page;

import java.util.Collection;
import java.util.List;

public interface DataSystemResourceService {
    
    /**
     * Get data system resource DTO by id.
     *
     * @param resourceId resource id
     * @return data system resource DTO
     */
    DataSystemResourceDTO getById(Long resourceId);
    
    /**
     * Get data system resource DTO by ids. Caution: you must to know all the type of result data system resources, each particular process of data system resource type must defined in data system
     * service interface.
     *
     * @param resourceIds resource ids
     * @return data system resource DTOs
     */
    List<DataSystemResourceDTO> getByIds(List<Long> resourceIds);
    
    /**
     * Get data system resource detail DTO by id.
     *
     * @param resourceId resource id
     * @return data system resource detail DTO
     */
    DataSystemResourceDetailDTO getDetailById(Long resourceId);
    
    /**
     * Get particular resource type sons of a resource. Caution: this method will only find in son source
     *
     * @param resourceId parent resource id
     * @param childrenResourceType children resource type
     * @return children resources
     */
    List<DataSystemResourceDTO> getChildren(Long resourceId, DataSystemResourceType childrenResourceType);
    
    /**
     * Get particular resource type sons of a resource. Caution: this method will only find in son source
     *
     * @param resourceId parent resource id
     * @param childrenResourceType children resource type
     * @return children resources
     */
    List<DataSystemResourceDetailDTO> getDetailChildren(Long resourceId, DataSystemResourceType childrenResourceType);
    
    /**
     * Get parent resource by resource type and specific configuration value.
     *
     * @param resourceId resource id
     * @param childrenResourceType parent resource type
     * @param configurationName configuration name
     * @param configurationValue configuration value
     * @return children resources
     */
    List<DataSystemResourceDetailDTO> getDetailChildren(Long resourceId, DataSystemResourceType childrenResourceType, String configurationName, String configurationValue);
    
    /**
     * Get data system type.
     *
     * @param resourceId resource id
     * @return data system type
     */
    DataSystemType getDataSystemType(Long resourceId);
    
    /**
     * Get parent resource by parent resource type.
     *
     * @param resourceId resource id
     * @param parentResourceType parent resource type
     * @return parent system resource
     */
    DataSystemResourceDTO getParent(Long resourceId, DataSystemResourceType parentResourceType);
    
    /**
     * Get parent resource by parent resource type.
     *
     * @param resourceId resource id
     * @param parentResourceType parent resource type
     * @return parent system resource
     */
    DataSystemResourceDetailDTO getDetailParent(Long resourceId, DataSystemResourceType parentResourceType);
    
    /**
     * Get all root resources.
     *
     * @return root resources
     */
    List<DataSystemResourceDTO> getAllRoots();
    
    /**
     * Query data system resource.
     *
     * @param query query
     * @return data system resource list
     */
    List<DataSystemResourceDTO> query(DataSystemResourceQuery query);
    
    /**
     * Query data system resource details.
     *
     * @param query query
     * @return data system resource list
     */
    List<DataSystemResourceDetailDTO> queryDetail(DataSystemResourceQuery query);
    
    /**
     * Paged query data system resource.
     *
     * @param query query
     * @return data system resource paged list
     */
    Page<DataSystemResourceDTO> pagedQuery(DataSystemResourceQuery query);
    
    /**
     * Paged query data system resource.
     *
     * @param query query
     * @return data system resource paged list
     */
    Page<DataSystemResourceDetailDTO> pagedQueryDetail(DataSystemResourceQuery query);
    
    /**
     * Merge all data system resources with the same parent on specific key.
     *
     * <p>
     * e.g. parent is cluster and all instances in the cluster as the param in this function, merge action means new instance's insert, exist ones' updated, removed ones' deleted
     * </p>
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResources data system resource like clusters, instances, databases, etc.
     * @param resourceType data system resource type
     * @param parentResourceId parent resource iDataSystemResourceDTOd
     * @return data system resources with ids
     */
    List<DataSystemResourceDetailDTO> mergeAllChildrenByName(List<DataSystemResourceDetailDTO> dataSystemResources, DataSystemResourceType resourceType, Long parentResourceId);
    
    /**
     * Merge all data system resources with the same parent on resource name.
     *
     * <p>
     * e.g. parent is cluster and all instances in the cluster as the param in this function,
     * merge action means new instance's insert, exist ones' updated, removed ones' deleted
     * </p>
     *
     * <p>
     * cautious: configurations will not be merged in existed resource,
     * and every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResources data system resource like clusters, instances, databases, etc.
     * @param resourceType data system resource type
     * @param parentResourceId parent resource iDataSystemResourceDTOd
     * @return data system resources with ids
     * @deprecated used by internal
     */
    @Deprecated
    List<DataSystemResourceDetailDTO> mergeAllChildrenByNameWithoutCheck(List<DataSystemResourceDetailDTO> dataSystemResources, DataSystemResourceType resourceType, Long parentResourceId);
    
    /**
     * Create data system resource.
     *
     * <p>
     * Before save, we will check resource configuration is valid.
     * </p>
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResourceDetail data system resource detail DTO
     * @return data system resource detail DTO
     */
    DataSystemResourceDetailDTO create(DataSystemResourceDetailDTO dataSystemResourceDetail);
    
    /**
     * Create data system resources.
     *
     * <p>
     * Before save, we will check resource configuration is valid.
     * </p>
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResourceDetails data system resource detail DTO
     * @return created data system resource detail DTO list
     */
    List<DataSystemResourceDetailDTO> batchCreate(List<DataSystemResourceDetailDTO> dataSystemResourceDetails);
    
    /**
     * Update data system resource.
     *
     * <p>
     * Before save, we will check resource configuration is valid.
     * </p>
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResourceDetail data system resource detail DTO
     * @return data system resource DTO
     */
    DataSystemResourceDetailDTO update(DataSystemResourceDetailDTO dataSystemResourceDetail);
    
    /**
     * Update data system resources.
     *
     * <p>
     * Before save, we will check resource configuration is valid.
     * </p>
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResourceDetails data system resource detail DTO
     * @return data system resource DTOs
     */
    List<DataSystemResourceDetailDTO> batchUpdate(List<DataSystemResourceDetailDTO> dataSystemResourceDetails);
    
    /**
     * Update data system resource.
     *
     * <p>
     * cautions: every sensitive configuration value will be encrypt before save.
     * </p>
     *
     * @param dataSystemResourceDetails data system resource detail DTOs
     * @return data system resource DTOs
     * @deprecated used by internal
     */
    @Deprecated
    List<DataSystemResourceDetailDTO> createOrUpdateAllWithoutCheck(List<DataSystemResourceDetailDTO> dataSystemResourceDetails);
    
    /**
     * Logical delete data system resources by id.
     *
     * @param id to logical delete id
     */
    void deleteById(Long id);
    
    /**
     * Logical delete data system resources by ids.
     *
     * @param ids to logical delete ids
     */
    void batchDeleteByIds(Collection<Long> ids);
    
    /**
     * Refresh a data system resource's dynamic children, such as database and table for MySQL.
     *
     * @param id resource root id
     */
    void refreshDynamicDataSystemResource(Long id);
}
