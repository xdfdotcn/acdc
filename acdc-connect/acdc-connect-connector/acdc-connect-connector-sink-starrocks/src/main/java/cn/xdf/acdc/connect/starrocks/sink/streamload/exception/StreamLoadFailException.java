package cn.xdf.acdc.connect.starrocks.sink.streamload.exception;

public class StreamLoadFailException extends RuntimeException {
    
    public StreamLoadFailException(String message) {
        super(message);
    }
    
    public StreamLoadFailException(String message, Throwable cause) {
        super(message, cause);
    }
}
