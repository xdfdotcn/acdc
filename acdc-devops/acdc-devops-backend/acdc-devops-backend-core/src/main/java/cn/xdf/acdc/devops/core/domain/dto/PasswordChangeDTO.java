package cn.xdf.acdc.devops.core.domain.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A DTO representing a password change required data - current and new password.
 */

@Getter
@Setter
@ToString
public class PasswordChangeDTO {

    private String currentPassword;

    private String newPassword;

    public PasswordChangeDTO() {
        // Empty constructor needed for Jackson.
    }

    public PasswordChangeDTO(final String currentPassword, final String newPassword) {
        this.currentPassword = currentPassword;
        this.newPassword = newPassword;
    }
}
