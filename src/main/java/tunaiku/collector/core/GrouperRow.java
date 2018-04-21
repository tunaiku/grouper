package tunaiku.collector.core;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import java.io.Serializable;
import java.util.List;


@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class GrouperRow implements Serializable{
    /**
     * Manually create a test row.
     */
    public static GrouperRow create(List<Object> fields) {
        return new AutoValue_GrouperRow(fields);
    }

    public abstract List<Object> fields();
}