package cn.xdf.acdc.connect.starrocks.sink.streamload;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public interface StreamLoadDataFormat {
    StreamLoadDataFormat JSON = new JSONFormat();
    StreamLoadDataFormat CSV = new CSVFormat();
    
    byte[] first();
    
    byte[] delimiter();
    
    byte[] end();
    
    class CSVFormat implements StreamLoadDataFormat, Serializable {
        
        private static final byte[] EMPTY_DELIMITER = new byte[0];
        private static final String DEFAULT_LINE_DELIMITER = "\n";
        private final byte[] delimiter;
        
        public CSVFormat() {
            this(DEFAULT_LINE_DELIMITER);
        }
        
        public CSVFormat(String rowDelimiter) {
            if (rowDelimiter == null) {
                throw new IllegalArgumentException("row delimiter can not be null");
            }
            
            this.delimiter = rowDelimiter.getBytes(StandardCharsets.UTF_8);
        }
        
        @Override
        public byte[] first() {
            return EMPTY_DELIMITER;
        }
        
        @Override
        public byte[] delimiter() {
            return delimiter;
        }
        
        @Override
        public byte[] end() {
            // For transaction stream load, need to append the row delimiter to the end of each
            // load, and separate the data in multiple loads. For example, there are 3 columns,
            // column delimiter ",", row delimiter "\n". There are 2 rows (1, 2, 3), (4, 5, 6),
            // and send them in two loads. If not append the end delimiter, the data received by
            // StarRocks will be "1,2,34,5,6", the two rows are not separated by row delimiter, and
            // StarRocks will parse it as one line with 5 columns. After appending the end delimiter,
            // it will be "1,2,3\n4,5,6", which will be parsed correctly
            return delimiter;
        }
        
    }
    
    class JSONFormat implements StreamLoadDataFormat, Serializable {
        private static final byte[] first = "[".getBytes(StandardCharsets.UTF_8);
        private static final byte[] delimiter = ",".getBytes(StandardCharsets.UTF_8);
        private static final byte[] end = "]".getBytes(StandardCharsets.UTF_8);
        
        @Override
        public byte[] first() {
            return first;
        }
        
        @Override
        public byte[] delimiter() {
            return delimiter;
        }
        
        @Override
        public byte[] end() {
            return end;
        }
        
    }
}
