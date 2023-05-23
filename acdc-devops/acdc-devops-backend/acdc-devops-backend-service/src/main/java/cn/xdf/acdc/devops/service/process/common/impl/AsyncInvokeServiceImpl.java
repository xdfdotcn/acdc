package cn.xdf.acdc.devops.service.process.common.impl;

import cn.xdf.acdc.devops.service.process.common.AsyncInvokeService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * 异步调用Service.
 *
 * @since 2022/8/8 3:01 下午
 */
@Service
public class AsyncInvokeServiceImpl implements AsyncInvokeService {
    
    @Async
    @Override
    public void asyncInvoke(final AsyncExec function) {
        function.execute();
    }
    
}
