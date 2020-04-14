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

/**
 * What does this class do? (construct child object of deserialised object if registered)
 * Why is it useful?  (write behaviour code into child class of generated code, so as not to lose said code upon regenerating the avro classes, and have this done automatically by the framework)
 *
 * https://martinfowler.com/dslCatalog/generationGap.html
 *
 * Separate generated code from non-generated code by inheritance.
 *
 * @param <T>
 * @param <R>
 */
public class GenerationGapAvroDeserializer<T extends SpecificRecord, R extends T> extends SpecificAvroDeserializer<R> {

  private KafkaAvroDeserializer inner;
  private Function<T, R> sss;
  private List<Class> clazzes;

  public GenerationGapAvroDeserializer() {
    this.clazzes = Lists.newArrayList();
  }

  public GenerationGapAvroDeserializer(List<Class> clazzes) {
    inner = new KafkaAvroDeserializer();
    this.clazzes = clazzes;
  }

  /**
   * For testing purposes only.
   */
  GenerationGapAvroDeserializer(final SchemaRegistryClient client) {
    inner = new KafkaAvroDeserializer(client);
  }

  public GenerationGapAvroDeserializer(Function<T, R> sss) {
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
