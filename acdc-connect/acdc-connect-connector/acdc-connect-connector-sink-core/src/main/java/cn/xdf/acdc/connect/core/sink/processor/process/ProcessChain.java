package cn.xdf.acdc.connect.core.sink.processor.process;

import org.apache.kafka.connect.data.Struct;

public interface ProcessChain {

    /**
     * Add process to the head of chain.
     *
     * @param process The process to be added
     * @return The chain itself
     */
    ProcessChain addFirst(Process process);

    /**
     * Add process to the last of chain.
     *
     * @param process The process to be added
     * @return The chain itself
     */
    ProcessChain addLast(Process process);

    /**
     * Process a record key or value struct as expect.
     *
     * @param struct record key or value struct{@link Struct} before process
     * @return record key or value struct after process
     */
    Struct process(Struct struct);

}
