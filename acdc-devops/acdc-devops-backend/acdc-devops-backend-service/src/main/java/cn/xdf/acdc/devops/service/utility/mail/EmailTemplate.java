package cn.xdf.acdc.devops.service.utility.mail;

import lombok.Getter;

@Getter
public enum EmailTemplate {
    
    SEND_APPROVAL("mail/connectionApproveEmail", "email.approve.tile"),
    
    SOURCE_OWNER_APPROVED("mail/connectionApproveEmail", "email.approve.tile"),
    
    SOURCE_OWNER_REFUSED("mail/connectionApproveEmail", "email.approve.tile"),
    
    DBA_APPROVED("mail/connectionApproveEmail", "email.approve.tile"),
    
    DBA_REFUSED("mail/connectionApproveEmail", "email.approve.tile"),
    
    METADATA_ALERT("mail/metadataCheckEmail", "email.metadata.alert");
    
    private String templateName;
    
    private String subjectI18nCode;
    
    EmailTemplate(final String templateName, final String subjectI18nCode) {
        this.templateName = templateName;
        this.subjectI18nCode = subjectI18nCode;
    }
}
