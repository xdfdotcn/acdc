package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Setter
@Accessors(chain = true)
public class EsDocField {

    private String name;

    private String type;
}
