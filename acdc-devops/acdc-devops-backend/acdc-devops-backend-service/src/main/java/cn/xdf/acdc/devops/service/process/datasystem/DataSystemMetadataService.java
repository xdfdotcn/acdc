package cn.xdf.acdc.devops.service.process.datasystem;

import java.util.List;

public interface DataSystemMetadataService<E> {

    /**
     * Refresh metadata.
     */
    void refreshMetadata();

    /**
     * Refresh metadata.
     *
     * @param freshElements specify elements to refresh
     */
    void refreshMetadata(List<E> freshElements);
}
