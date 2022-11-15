package cn.xdf.acdc.connect.core.util.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@AllArgsConstructor
@Getter
public class DestinationConfig {

    private String name;

    private Set<String> fieldsWhitelist;

    private Map<String, String> fieldsMapping;

    private Map<String, String> fieldsToAdd;

    private String rowFilterExpress;

    private DeleteMode deleteMode;

    private String logicalDeleteFieldName;

    private String logicalDeleteFieldValueDeleted;

    private String logicalDeleteFieldValueNormal;

}
