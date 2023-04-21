package cn.xdf.acdc.devops.service.utility.mail;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring5.SpringTemplateEngine;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Service for sending emails. We use the {@link Async} annotation to send emails asynchronously.
 */
@Service
@Slf4j
public class MailUtilityService {

    private static final String VARIABLE_TO_KEY = "to";

    private static final String VARIABLE_CC_KEY = "cc";

    private final JavaMailSender javaMailSender;

    private final MessageSource messageSource;

    private final SpringTemplateEngine templateEngine;

    public MailUtilityService(
            final JavaMailSender javaMailSender,
            final MessageSource messageSource,
            final SpringTemplateEngine templateEngine
    ) {
        this.javaMailSender = javaMailSender;
        this.messageSource = messageSource;
        this.templateEngine = templateEngine;
    }

    /**
     * Send email.
     *
     * @param form        from
     * @param to          to
     * @param cc          cc
     * @param subject     subject
     * @param content     content
     * @param isMultipart isMultipart
     * @param isHtml      isHtml
     */
    public void sendEmail(
            final String form,
            final List<String> to,
            final List<String> cc,
            final String subject,
            final String content,
            final boolean isMultipart,
            final boolean isHtml) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(to), "addressee must not be empty.");
        //Preconditions.checkArgument(!CollectionUtils.isEmpty(cc), "Cc people must not be empty.");
        log.debug(
                "Send email[multipart '{}' and html '{}'] to '{}' with subject '{}' and content={}",
                isMultipart,
                isHtml,
                to,
                subject,
                content
        );

        // Prepare message using a Spring helper
        MimeMessage mimeMessage = javaMailSender.createMimeMessage();
        try {
            MimeMessageHelper message = new MimeMessageHelper(mimeMessage, isMultipart, StandardCharsets.UTF_8.name());

            // cc
            if (!CollectionUtils.isEmpty(cc)) {
                String[] ccEmailArray = cc.toArray(new String[cc.size()]);
                message.setCc(ccEmailArray);
            }

            // to
            String[] toEmailArray = to.toArray(new String[to.size()]);
            message.setTo(toEmailArray);

            // from
            message.setFrom(form);

            // subject
            message.setSubject(subject);
            message.setText(content, isHtml);

            // send email
            javaMailSender.send(mimeMessage);
            log.debug("Sent email to User '{}'", to);
        } catch (MailException | MessagingException e) {
            log.warn("Email could not be sent to user '{}'", to, e);
        }
    }

    /**
     * Send email from template.
     *
     * @param from            from
     * @param to              to
     * @param cc              cc
     * @param subjectI18nCode titleKey
     * @param templateName    template
     * @param model           model
     */
    public void sendEmailFromTemplate(
            final String from,
            final List<DomainUser> to,
            final List<DomainUser> cc,
            final String subjectI18nCode,
            final String templateName,
            final Object model
    ) {
        Preconditions.checkNotNull(model, "Email model must not be null.");
        Preconditions.checkArgument(!CollectionUtils.isEmpty(to), "The receiver  must not be empty.");
        List<DomainUser> ccList = CollectionUtils.isEmpty(cc) ? Collections.EMPTY_LIST : cc;

        Locale locale = Locale.getDefault();
        Context context = new Context(locale);
        String variableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, model.getClass().getSimpleName());
        context.setVariable(variableName, model);
        context.setVariable(VARIABLE_TO_KEY, to);
        context.setVariable(VARIABLE_CC_KEY, ccList);

        String content = templateEngine.process(templateName, context);
        String subject = messageSource.getMessage(subjectI18nCode, null, locale);

        List<String> emailTo = to.stream().map(it -> it.getEmail()).collect(Collectors.toList());
        List<String> emailCc = ccList.stream().map(it -> it.getEmail()).collect(Collectors.toList());

        sendEmail(from, emailTo, emailCc, subject, content, false, true);
    }
}
