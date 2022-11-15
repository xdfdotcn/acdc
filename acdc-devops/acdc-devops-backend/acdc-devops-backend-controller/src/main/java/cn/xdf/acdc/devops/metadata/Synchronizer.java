package cn.xdf.acdc.devops.metadata;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

// CHECKSTYLE:OFF

@Profile({"prod", "uat"})
@Component
@Slf4j
public class Synchronizer {

    @Autowired(required = false)
    private List<DataSystemMetadataService> metadataJobs;

    /**
     * Refresh metadata for ACDC.
     */
    @Scheduled(fixedRateString = "${scheduler.refresh.metadata.interval.ms:3600000}")
    @Timed(description = "refresh rdb metadata")
    public void refreshMetadata() {
        List<Exception> exceptions = new ArrayList<>();
        if (metadataJobs == null) {
            log.info("External metadata source is not set, ignore.");
            return;
        }
        metadataJobs.forEach(metadataJob -> {
            try {
                metadataJob.refreshMetadata();
            } catch (Exception exception) {
                log.error("exception:{}, stack trace:{}", exception, exception.getStackTrace());
                exceptions.add(exception);
            }
        });

        if (!exceptions.isEmpty()) {
            throw new ServerErrorException(exceptions.toString());
        }
    }
}
