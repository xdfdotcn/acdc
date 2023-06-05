package cn.xdf.acdc.devops.service.util;

import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

public final class UIError {
    
    private UIError() {
    }
    
    /**
     * 返回给 UI 错误消息体.
     *
     * @param status http status
     * @param errorMsg error message
     * @return map
     */
    public static Map<String, Object> getDefaultStyleMsg(final HttpStatus status, final String errorMsg) {
        Map<String, Object> map = new HashMap<>();
        map.put("errorMessage", errorMsg);
        map.put("errorCode", status.value());
        map.put("success", false);
        map.put("showType", 4);
        return map;
    }
    
    /**
     * 生成简化版的错误信息 (简化,不带有错误提示类型).
     *
     * @param status http status
     * @param errorMsg error message
     * @return map
     */
    public static Map<String, Object> getBriefStyleMsg(final HttpStatus status, final String errorMsg) {
        Map<String, Object> map = new HashMap<>();
        map.put("errorMessage", errorMsg);
        map.put("errorCode", status.value());
        map.put("success", false);
        return map;
    }
}
