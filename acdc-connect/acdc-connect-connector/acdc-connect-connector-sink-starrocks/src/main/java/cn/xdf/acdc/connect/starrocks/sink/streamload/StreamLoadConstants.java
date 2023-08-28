package cn.xdf.acdc.connect.starrocks.sink.streamload;

public interface StreamLoadConstants {
    
    String PATH_STREAM_LOAD = "/api/{db}/{table}/_stream_load";
    
    String PATH_TRANSACTION_BEGIN = "/api/transaction/begin";
    String PATH_TRANSACTION_SEND = "/api/transaction/load";
    String PATH_TRANSACTION_ROLLBACK = "/api/transaction/rollback";
    String PATH_TRANSACTION_PRE_COMMIT = "/api/transaction/prepare";
    String PATH_TRANSACTION_COMMIT = "/api/transaction/commit";
    
    String PATH_STREAM_LOAD_STATE = "/api/{db}/get_load_state?label={label}";
    
    String RESULT_STATUS_OK = "OK";
    String RESULT_STATUS_SUCCESS = "Success";
    String RESULT_STATUS_FAILED = "Fail";
    String RESULT_STATUS_LABEL_EXISTED = "Label Already Exists";
    String RESULT_STATUS_TRANSACTION_NOT_EXISTED = "TXN_NOT_EXISTS";
    String RESULT_STATUS_TRANSACTION_COMMIT_TIMEOUT = "Commit Timeout";
    String RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT = "Publish Timeout";
    
    String EXISTING_JOB_STATUS_FINISHED = "FINISHED";
    
    String LABEL_STATE_VISIBLE = "VISIBLE";
    String LABEL_STATE_COMMITTED = "COMMITTED";
    String LABEL_STATE_PREPARED = "PREPARED";
    String LABEL_STATE_PREPARE = "PREPARE";
    String LABEL_STATE_ABORTED = "ABORTED";
    String LABEL_STATE_UNKNOWN = "UNKNOWN";
    
    public static String getBeginUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        
        return host + StreamLoadConstants.PATH_TRANSACTION_BEGIN;
    }
    
    public static String getSendUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        
        return host + StreamLoadConstants.PATH_TRANSACTION_SEND;
    }
    
    public static String getPrepareUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_PRE_COMMIT;
    }
    
    public static String getCommitUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_COMMIT;
    }
    
    public static String getRollbackUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_ROLLBACK;
    }
}
