package cn.xdf.acdc.connect.core.sink.data;

import cn.xdf.acdc.connect.core.util.Constants;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.connect.data.Schema;

@Getter
@Setter
public class TemporaryFieldAndValue {

    private Schema schema;

    private String name;

    private Object value;

    public TemporaryFieldAndValue(final Schema schema, final String name, final Object value) {
        this.schema = schema;
        this.name = name;
        this.value = value;
    }

    /**
     * Get the field is metadata field or not.
     * Metadata field have a common prefix in name.
     *
     * @return true when it's metadata field
     */
    public boolean isMetaDataField() {
        return name.startsWith(Constants.META_DATA_FIELD_NAME_PREFIX);
    }

}
