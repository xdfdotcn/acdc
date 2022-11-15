package cn.xdf.acdc.devops.service.utility.mail;

import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import cn.xdf.acdc.devops.service.config.ACDCEmailConfig;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DefaultEmailSender implements EmailSender {

    @Autowired
    private ACDCEmailConfig emailConfig;

    @Autowired
    private MailUtilityService mailService;

    @Autowired
    private ACDCEmailConfig acdcEmailConfig;

    @Override
    public void sendInnerWarningEmail(final EmailTemplate template, final Object content) {
        DomainUserDTO acdcGroup = new DomainUserDTO(acdcEmailConfig.getCcEmailAddress());
        this.sendEmail(Lists.newArrayList(acdcGroup), new ArrayList<>(), template, content);
    }

    @Override
    public void sendEmail(
            final List<DomainUserDTO> to,
            final List<DomainUserDTO> cc,
            final EmailTemplate template,
            final Object content) {
        String from = emailConfig.getFromEmailAddress();
        String templateName = template.getTemplateName();
        String subjectI18nCode = template.getSubjectI18nCode();
        mailService.sendEmailFromTemplate(from, to, cc, subjectI18nCode, templateName, content);
    }
}
