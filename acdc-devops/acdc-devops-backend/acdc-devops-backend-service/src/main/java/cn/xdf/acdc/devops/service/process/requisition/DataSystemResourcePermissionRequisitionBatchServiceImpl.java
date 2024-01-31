package cn.xdf.acdc.devops.service.process.requisition;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionBatchQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionBatchRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Transactional
@Service
public class DataSystemResourcePermissionRequisitionBatchServiceImpl implements DataSystemResourcePermissionRequisitionBatchService {
    
    private final DataSystemResourcePermissionRequisitionBatchRepository dataSystemResourcePermissionRequisitionBatchRepository;
    
    private final DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository;
    
    public DataSystemResourcePermissionRequisitionBatchServiceImpl(final DataSystemResourcePermissionRequisitionBatchRepository dataSystemResourcePermissionRequisitionBatchRepository,
                                                                   final DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository) {
        this.dataSystemResourcePermissionRequisitionBatchRepository = dataSystemResourcePermissionRequisitionBatchRepository;
        this.dataSystemResourcePermissionRequisitionRepository = dataSystemResourcePermissionRequisitionRepository;
    }
    
    @Override
    public List<DataSystemResourcePermissionRequisitionBatchDTO> query(final DataSystemResourcePermissionRequisitionBatchQuery query) {
        return dataSystemResourcePermissionRequisitionBatchRepository.query(query).stream().map(DataSystemResourcePermissionRequisitionBatchDTO::new)
                .collect(Collectors.toList());
    }
    
    @Override
    public void updateState(final Long batchId, final ApprovalBatchState state) {
        DataSystemResourcePermissionRequisitionBatchDO batchDO = dataSystemResourcePermissionRequisitionBatchRepository.findById(batchId).orElseThrow(
                () -> new ServerErrorException(String.format("Requisition batch could not be found with id: %s.", batchId))
        );
        batchDO.setState(state);
        dataSystemResourcePermissionRequisitionBatchRepository.save(batchDO);
    }
    
    @Override
    public Long create(final Long userId, final String description, final Long sinkProjectId, final Map<Long, Long> dataSystemResourceIdProjectIdMap) {
        
        Map<String, DataSystemResourcePermissionRequisitionDO> permissionRequisitions = new HashMap<>();
        dataSystemResourceIdProjectIdMap.forEach((dataSystemResourceId, sourceProjectId) -> {
            Optional<DataSystemResourcePermissionRequisitionDO> relatedOptional = getRelatedPermissionRequisition(sinkProjectId, dataSystemResourceId, sourceProjectId);
            
            if (relatedOptional.isPresent()) {
                DataSystemResourcePermissionRequisitionDO relatedOne = relatedOptional.get();
                String uk = String.join(Symbol.CABLE, relatedOne.getId().toString(), sourceProjectId.toString());
                permissionRequisitions.putIfAbsent(uk, relatedOne);
            } else {
                String uk = String.join(Symbol.CABLE, Symbol.EMPTY, sourceProjectId.toString());
                DataSystemResourcePermissionRequisitionDO resourcePermissionRequisition = getNewPermissionRequisition(userId, description, sinkProjectId, dataSystemResourceId, sourceProjectId);
                
                if (permissionRequisitions.containsKey(uk)) {
                    permissionRequisitions.get(uk).getDataSystemResources()
                            .addAll(resourcePermissionRequisition.getDataSystemResources());
                } else {
                    permissionRequisitions.put(uk, resourcePermissionRequisition);
                }
            }
        });
        List<DataSystemResourcePermissionRequisitionDO> permissionRequisitionResult =
                dataSystemResourcePermissionRequisitionRepository.saveAll(permissionRequisitions.values());
        
        DataSystemResourcePermissionRequisitionBatchDO batchDO = getBatchDO(userId, description, permissionRequisitionResult);
        return dataSystemResourcePermissionRequisitionBatchRepository.save(batchDO).getId();
    }
    
    @NotNull
    private DataSystemResourcePermissionRequisitionBatchDO getBatchDO(final Long userId, final String description, final List<DataSystemResourcePermissionRequisitionDO> permissionRequisitionResult) {
        DataSystemResourcePermissionRequisitionBatchDO batchDO = new DataSystemResourcePermissionRequisitionBatchDO();
        batchDO.setUser(new UserDO(userId));
        batchDO.setDescription(description);
        batchDO.setState(ApprovalBatchState.PENDING);
        batchDO.setPermissionRequisitions(new HashSet<>(permissionRequisitionResult));
        return batchDO;
    }
    
    @NotNull
    private DataSystemResourcePermissionRequisitionDO getNewPermissionRequisition(final Long userId,
                                                                                  final String description, final Long sinkProjectId, final Long dataSystemResourceId, final Long sourceProjectId) {
        DataSystemResourcePermissionRequisitionDO resourcePermissionRequisition = new DataSystemResourcePermissionRequisitionDO()
                .setDescription(description)
                .setUser(new UserDO(userId))
                .setSinkProject(new ProjectDO(sinkProjectId))
                .setSourceProject(new ProjectDO(sourceProjectId))
                .setState(ApprovalState.APPROVING);
        resourcePermissionRequisition.setDataSystemResources(Sets.newHashSet(
                new DataSystemResourceDO(dataSystemResourceId)
        ));
        return resourcePermissionRequisition;
    }
    
    @NotNull
    private Optional<DataSystemResourcePermissionRequisitionDO> getRelatedPermissionRequisition(final Long sinkProjectId, final Long dataSystemResourceId, final Long sourceProjectId) {
        List<DataSystemResourcePermissionRequisitionDO> existPermissions =
                dataSystemResourcePermissionRequisitionRepository.findBySourceProjectIdAndDataSystemResourceId(sourceProjectId, sinkProjectId, dataSystemResourceId);
        return existPermissions.stream()
                .filter(existPermission -> !ApprovalState.REFUSED_STATES.contains(existPermission.getState()))
                .collect(Collectors.toSet()).stream().findFirst();
    }
}
