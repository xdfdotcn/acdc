package cn.xdf.acdc.devops.service.utility.mail;

import cn.xdf.acdc.devops.service.config.ACDCEmailProperties;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DefaultEmailSender implements EmailSender {
    
    @Autowired
    private ACDCEmailProperties emailConfig;
    
    @Autowired
    private MailUtilityService mailService;
    
    @Autowired
    private ACDCEmailProperties acdcEmailProperties;
    
    @Override
    public void sendInnerWarningEmail(final EmailTemplate template, final Object content) {
        DomainUser acdcGroup = new DomainUser(acdcEmailProperties.getCcEmailAddress());
        this.sendEmail(Lists.newArrayList(acdcGroup), new ArrayList<>(), template, content);
    }
    
    @Override
    public void sendEmail(
            final List<DomainUser> to,
            final List<DomainUser> cc,
            final EmailTemplate template,
            final Object content) {
        String from = emailConfig.getFromEmailAddress();
        String templateName = template.getTemplateName();
        String subjectI18nCode = template.getSubjectI18nCode();
        mailService.sendEmailFromTemplate(from, to, cc, subjectI18nCode, templateName, content);
    }
}
