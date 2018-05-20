import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SensorDatenSerializer implements Serializer<SensorDaten> {

  private boolean isKey;

  @Override
  public void configure(Map configs, boolean isKey) {
    setKey(isKey);
  }

  @Override
  public byte[] serialize(String topic, SensorDaten data) {

    try {
      return SerializationUtils.serialize(data);
    } catch (RuntimeException e) {
      throw new SerializationException("Error serializing value", e);
    }
  }

  @Override
  public void close() {}

  public boolean isKey() {
    return isKey;
  }

  public void setKey(boolean key) {
    isKey = key;
  }
}
