// Must be in this package as the exnteded class is package private
package io.confluent.kafka.streams.serdes.avro;

import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class WrappingSpecificAvroDeserializer<T extends SpecificRecord, R extends T> extends SpecificAvroDeserializer<R> {

  private KafkaAvroDeserializer inner;
  private Function<T, R> sss;
  private List<Class> clazzes;

  public WrappingSpecificAvroDeserializer() {
    this.clazzes = Lists.newArrayList();
  }

  public WrappingSpecificAvroDeserializer(List<Class> clazzes) {
    inner = new KafkaAvroDeserializer();
    this.clazzes = clazzes;
  }

  /**
   * For testing purposes only.
   */
  WrappingSpecificAvroDeserializer(final SchemaRegistryClient client) {
    inner = new KafkaAvroDeserializer(client);
  }

  public WrappingSpecificAvroDeserializer(Function<T, R> sss) {
    this.sss = sss;
  }

  @Override
  public void configure(final Map<String, ?> deserializerConfig,
                        final boolean isDeserializerForRecordKeys) {
    inner.configure(
            ConfigurationUtils.withSpecificAvroEnabled(deserializerConfig),
            isDeserializerForRecordKeys);
  }

  @SneakyThrows
  @Override
  public R deserialize(final String topic, final byte[] bytes) {
    R deserialize = (R) inner.deserialize(topic, bytes);
    Class<? extends SpecificRecord> deserializeClass = deserialize.getClass();

    for (Class c : this.clazzes) {
      try {
        if (deserializeClass.isAssignableFrom(c)) {
          R t = (R) c.getDeclaredConstructor(deserializeClass).newInstance(deserialize);
          return t;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return deserialize;
  }

  @Override
  public void close() {
    inner.close();
  }

}
