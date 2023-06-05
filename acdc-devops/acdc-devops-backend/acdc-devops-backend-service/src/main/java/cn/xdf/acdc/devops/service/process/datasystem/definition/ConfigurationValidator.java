package cn.xdf.acdc.devops.service.process.datasystem.definition;

@FunctionalInterface
public interface ConfigurationValidator {
    
    /**
     * Validate input value.
     *
     * @param value the value that needs to be validated
     * @return true: verification pass, false: verification fail
     */
    boolean validate(String value);
}
