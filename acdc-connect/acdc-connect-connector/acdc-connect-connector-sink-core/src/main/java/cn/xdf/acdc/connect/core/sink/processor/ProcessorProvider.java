package cn.xdf.acdc.connect.core.sink.processor;

public interface ProcessorProvider<T extends Processor> {

    /**
     * Get processor for the specific destination.
     *
     * @param destination destination name
     * @return processor for the destination
     */
    T getProcessor(String destination);

}
