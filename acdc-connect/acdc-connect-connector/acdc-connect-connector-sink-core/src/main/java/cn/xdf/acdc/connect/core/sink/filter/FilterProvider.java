package cn.xdf.acdc.connect.core.sink.filter;

public interface FilterProvider {

    /**
     * Get filter for specific the destination.
     *
     * @param destination destination name
     * @return filter for the destination
     */
    Filter getFilter(String destination);

}
