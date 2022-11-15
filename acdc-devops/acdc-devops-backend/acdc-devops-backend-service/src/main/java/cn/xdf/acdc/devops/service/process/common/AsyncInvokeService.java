package cn.xdf.acdc.devops.service.process.common;

/**
 * 异步调用Service.
 *
 */
public interface AsyncInvokeService {

    /**
     * 异步调用.
     *
     * @param function 方法体
     * @date 2022/9/8 5:37 下午
     */
    void asyncInvoke(AsyncExec function);

    @FunctionalInterface
    interface AsyncExec {
        /**
         * 执行方法体.
         *
         * @date 2022/9/8 5:38 下午
         */
        void execute();
    }

}
