package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import org.springframework.scheduling.TaskScheduler;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class WideTableInformer extends AbstractFixedRateRunnableInformer<WideTableDTO> {
    
    private final WideTableService wideTableService;
    
    public WideTableInformer(final TaskScheduler taskScheduler, final WideTableService wideTableService) {
        super(taskScheduler);
        this.wideTableService = wideTableService;
    }
    
    @Override
    List<WideTableDTO> query() {
        WideTableQuery wideTableQuery = new WideTableQuery()
                .setBeginUpdateTime(super.getLastUpdateTime());
        return wideTableService.query(wideTableQuery);
    }
    
    @Override
    Long getKey(final WideTableDTO element) {
        return element.getId();
    }
    
    @Override
    boolean equals(final WideTableDTO e1, final WideTableDTO e2) {
        return Objects.equals(e1, e2);
    }
    
    @Override
    boolean isDeleted(final WideTableDTO older, final WideTableDTO newer) {
        return !older.isDeleted() && newer.isDeleted();
    }
    
    @Override
    Date getUpdateTime(final WideTableDTO approvedWideTableDTO) {
        return approvedWideTableDTO.getUpdateTime();
    }
}
