package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;

import java.util.Map;

public interface Process {

    /**
     * Do process for input args.
     *
     * @param fieldAndValues field name to {@link TemporaryFieldAndValue} mapping
     * @return go on next process or not
     */
    boolean execute(Map<String, TemporaryFieldAndValue> fieldAndValues);

}


