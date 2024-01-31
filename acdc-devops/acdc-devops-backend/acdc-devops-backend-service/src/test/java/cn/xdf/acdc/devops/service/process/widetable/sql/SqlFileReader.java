package cn.xdf.acdc.devops.service.process.widetable.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SqlFileReader {
    
    private static final String SQL_FILE_ROOT_PATH = "/sql/";
    
    private static final String LINE_BREAK = "\n";
    
    private String baseDirName = "";
    
    public SqlFileReader(final String baseDirName) {
        this.baseDirName = baseDirName;
    }
    
    /**
     * 根据 sql 文件名称读取 sql 内容.
     *
     * @param sqlFileName sql 文件名
     * @return sql 语句
     */
    public String read(final String sqlFileName) {
        String sqlFilePath = createSqlFilePath(sqlFileName);
        InputStream input = SqlFileReader.class.getResourceAsStream(sqlFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder sqlBuilder = new StringBuilder();
        String line = "";
        
        try {
            // CHECKSTYLE:OFF
            for (; (line = reader.readLine()) != null; ) {
                sqlBuilder.append(line);
                sqlBuilder.append(LINE_BREAK);
            }
            // CHECKSTYLE:ON
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            
            try {
                if (reader != null) {
                    reader.close();
                }
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return sqlBuilder.toString();
    }
    
    /**
     * 根据 测试方法名称 读取 sql 内容.
     *
     * @param caseName 测试 case 名称
     * @return TestCaseSql
     */
    public TestCaseSql readTestCase(final String caseName) {
        String sqlFilePath = createSqlFilePath(caseName);
        InputStream input = SqlFileReader.class.getResourceAsStream(sqlFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder originalSqlBuilder = new StringBuilder();
        StringBuilder optimizedSqlBuilder = new StringBuilder();
        boolean readToOriginalSqlBuilder = false;
        boolean readToOptimizedSqlBuilder = false;
        String line = "";
        try {
            // CHECKSTYLE:OFF
            for (; (line = reader.readLine()) != null; ) {
                String trimLine = line.trim();
                if (trimLine.startsWith("-- original")) {
                    readToOriginalSqlBuilder = true;
                    readToOptimizedSqlBuilder = false;
                    continue;
                }
                if (trimLine.startsWith("-- optimized")) {
                    readToOriginalSqlBuilder = false;
                    readToOptimizedSqlBuilder = true;
                    continue;
                }
                if (readToOriginalSqlBuilder) {
                    originalSqlBuilder.append(line);
                    originalSqlBuilder.append(LINE_BREAK);
                }
                if (readToOptimizedSqlBuilder) {
                    optimizedSqlBuilder.append(line);
                    optimizedSqlBuilder.append(LINE_BREAK);
                }
            }
            // CHECKSTYLE:ON
            return new TestCaseSql(originalSqlBuilder.toString(), optimizedSqlBuilder.toString());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
    
    private String createSqlFilePath(final String name) {
        String sqlFilePath = SQL_FILE_ROOT_PATH + baseDirName + "/" + name + ".sql";
        return sqlFilePath;
    }
    
    public class TestCaseSql {
        
        private String originalSql;
        
        private String optimizedSql;
        
        public TestCaseSql(final String originalSql, final String optimizedSql) {
            this.originalSql = originalSql;
            this.optimizedSql = optimizedSql;
        }
        
        public String getOriginalSql() {
            return this.originalSql;
        }
        
        public String getOptimizedSql() {
            return optimizedSql;
        }
    }
}
