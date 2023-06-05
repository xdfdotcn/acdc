package cn.xdf.acdc.devops.service.process.check;

import java.util.List;
import java.util.Map;

public interface CheckerInOrder {
    
    /**
     * Analyze metadata and return message which incomplete or enabled and should be alerted.
     *
     * @return message, key: reason, value: data list.
     */
    Map<String, List<String>> checkMetadataAndReturnErrorMessage();
}
