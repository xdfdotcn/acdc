package cn.xdf.acdc.connect.starrocks.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Version {
    
    private static final String PATH = "/starrocks-connect-kafka-version.properties";
    
    private static String version = "unknown";
    
    static {
        try (InputStream stream = Version.class.getResourceAsStream(PATH)) {
            Properties props = new Properties();
            props.load(stream);
            version = props.getProperty("version", version).trim();
        } catch (IOException e) {
            log.warn("Error while loading version:", e);
        }
    }
    
    /**
     * Get version.
     *
     * @return version
     */
    public static String getVersion() {
        return version;
    }
}
