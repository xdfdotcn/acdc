package cn.xdf.acdc.devops.service.process.sync;

/**
 * Synchronize data from external systems like databases or tables. Here we use spring {@link org.springframework.core.annotation.Order} to keep the function's order.
 */
public interface SynchronizerInOrder {

    /**
     * sync meta data.
     */
    void sync();
}
