package cn.xdf.acdc.devops.aop;

import cn.xdf.acdc.devops.service.utility.mail.DefaultEmailSender;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import com.google.common.collect.Lists;
import io.jsonwebtoken.lang.Maps;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
@Aspect
@Slf4j
public class SchedulerExceptionAlertAspect {
    
    private static final String SCHEDULER_EXCEPTION_TITLE = "元数据同步定时任务执行异常";
    
    @Autowired
    private DefaultEmailSender emailSender;
    
    /**
     * A point cut.
     */
    @Pointcut("execution(* cn.xdf.acdc.devops.service.process.sync.SynchronizerInOrder.*(..))")
    public void all() {
    }
    
    /**
     * After throwing.
     *
     * @param exception exception
     */
    @AfterThrowing(pointcut = "all()", throwing = "exception")
    public void afterThrowing(final Exception exception) {
        List<String> exceptionMessage = Lists.newArrayList(exception.toString(), Arrays.toString(exception.getStackTrace()));
        List<Map<String, List<String>>> messages =
                Lists.newArrayList(Maps.of(SCHEDULER_EXCEPTION_TITLE, exceptionMessage).build());
        emailSender.sendInnerWarningEmail(EmailTemplate.METADATA_ALERT, messages);
    }
}
