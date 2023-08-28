package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import com.google.common.base.Strings;

public class UrlUtil {
    
    private static final String JDBC_URL_FORMAT = "jdbc:%s://%s:%d";
    
    private static final String HTTP_URL_FORMAT = "%s:%d";
    
    /**
     * Generate jdbc schema url.
     *
     * @param dataSystemType data system type
     * @param host host
     * @param port port
     * @param database database
     * @param properties jdbc properties
     * @return url in jdbc schema
     */
    public static String generateJDBCUrl(final String dataSystemType, final String host, final int port, final String database, final String properties) {
        StringBuilder url = new StringBuilder(String.format(JDBC_URL_FORMAT, dataSystemType, host, port));
        
        if (!Strings.isNullOrEmpty(database)) {
            url.append(CommonConstant.PATH_SEPARATOR).append(database);
        }
        
        if (!Strings.isNullOrEmpty(properties)) {
            url.append(CommonConstant.QUESTION_MARK).append(properties);
        }
        
        return url.toString();
    }
    
    /**
     * Generate jdbc schema url.
     *
     * @param dataSystemType data system type
     * @param host host
     * @param port port
     * @param database database
     * @return url in jdbc schema
     */
    public static String generateJDBCUrl(final String dataSystemType, final String host, final int port, final String database) {
        return generateJDBCUrl(dataSystemType, host, port, database, SystemConstant.EMPTY_STRING);
    }
    
    /**
     * Generate http url.
     *
     * @param ip ip
     * @param port port
     * @return http url
     */
    public static String generateHttpUrl(final String ip, final int port) {
        return String.format(HTTP_URL_FORMAT, ip, port);
    }
}
