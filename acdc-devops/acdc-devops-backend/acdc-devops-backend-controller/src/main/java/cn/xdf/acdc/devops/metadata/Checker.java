package cn.xdf.acdc.devops.metadata;

import cn.xdf.acdc.devops.service.process.check.CheckerInOrder;
import cn.xdf.acdc.devops.service.utility.mail.DefaultEmailSender;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Profile("prod")
@Component
@Slf4j
public class Checker {

    @Autowired(required = false)
    private List<CheckerInOrder> checkerInOrderList;

    @Autowired
    private DefaultEmailSender emailSender;

    /**
     * Alert to incomplete metadata, default interval: 24h.
     */
    @Scheduled(fixedRateString = "${scheduler.metadata.alert.interval.ms:86400000}")
    @Timed(description = "metadata checker")
    public void metadataChecker() {
        log.info("Begin to check acdc metadata");
        this.checkMetadata();
    }

    private void checkMetadata() {
        if (checkerInOrderList == null) {
            log.info("Metadata checker is not Implemented.");
            return;
        }

        List<Map<String, List<String>>> messages = checkerInOrderList.stream()
                .map(CheckerInOrder::checkMetadataAndReturnErrorMessage)
                .filter(map -> map != null && !map.isEmpty())
                .collect(Collectors.toList());

        if (!messages.isEmpty()) {
            log.warn("metadata check messages: {}", messages);
            emailSender.sendInnerWarningEmail(EmailTemplate.METADATA_ALERT, messages);
        }
    }
}
