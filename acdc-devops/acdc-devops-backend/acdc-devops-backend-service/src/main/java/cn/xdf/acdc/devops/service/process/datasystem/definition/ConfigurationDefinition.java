package cn.xdf.acdc.devops.service.process.datasystem.definition;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ConfigurationDefinition<T> {

    private boolean isOptional;

    private boolean isSensitive;

    private String name;

    private String description;

    private T defaultValue;

    private ConfigurationValueType valueType;

    private T[] availableValues;

    private ConfigurationValidator validator;
}
