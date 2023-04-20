package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsernameAndPassword {

    private String username;

    private String password;
}
