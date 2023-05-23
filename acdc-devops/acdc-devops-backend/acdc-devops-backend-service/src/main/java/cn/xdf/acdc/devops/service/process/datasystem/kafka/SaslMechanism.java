package cn.xdf.acdc.devops.service.process.datasystem.kafka;

public enum SaslMechanism {
    SCRAM_SHA_512("SCRAM-SHA-512"), SCRAM_SHA_256("SCRAM-SHA-256"), PLAIN("PLAIN");
    
    // Because we can not use '-' in a enum instance name, so add a property named name.
    private final String name;
    
    SaslMechanism(final String name) {
        this.name = name;
    }
    
    /**
     * Get name of sasl mechanism.
     *
     * @return name of sasl mechanism
     */
    public String getName() {
        return name;
    }
    
    // Override toString method to use name property instead of enum instance name
    @Override
    public String toString() {
        return this.name;
    }
}
