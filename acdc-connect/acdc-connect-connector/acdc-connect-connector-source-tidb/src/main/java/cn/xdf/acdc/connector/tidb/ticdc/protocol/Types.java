package cn.xdf.acdc.connector.tidb.ticdc.protocol;

/**
 * Ticdc open protocol column type code.
 * See https://docs.pingcap.com/tidb/stable/ticdc-open-protocol#column-type-code
 */
public class Types {
    
    public static final int BOOL_OR_TINYINT = 1;

    public static final int SMALLINT = 2;

    public static final int INT = 3;

    public static final int FLOAT = 4;

    public static final int DOUBLE = 5;

    public static final int NULL = 6;

    public static final int TIMESTAMP = 7;

    public static final int BIGINT = 8;

    public static final int MEDIUMINT = 9;

    public static final int DATE_V1 = 10;

    public static final int DATE_V2 = 14;

    public static final int TIME = 11;

    public static final int DATETIME = 12;

    public static final int YEAR = 13;

    public static final int VARCHAR_OR_VARBINARY_V1 = 15;

    public static final int BIT = 16;

    public static final int VARCHAR_OR_VARBINARY_V2 = 253;

    public static final int JSON = 245;

    public static final int DECIMAL = 246;

    public static final int ENUM = 247;

    public static final int SET = 248;

    public static final int TINYTEXT_OR_TINYBLOB = 249;

    public static final int MEDIUMTEXT_OR_MEDIUMBLOB = 250;

    public static final int LONGTEXT_OR_LONGBLOB = 251;

    public static final int TEXT_OR_BLOB = 252;

    public static final int CHAR_OR_BINARY = 254;

    public static final int GEOMETRY = 255;

}
