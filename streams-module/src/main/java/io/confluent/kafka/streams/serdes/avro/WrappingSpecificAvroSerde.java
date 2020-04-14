// Must be in this package as the exnteded class is package private
package io.confluent.kafka.streams.serdes.avro;

import io.confluent.ps.streams.referenceapp.chat.model.GoalEventWrapped;
import io.confluent.ps.streams.referenceapp.finance.model.HighLowBD;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class WrappingSpecificAvroSerde<T extends R, R extends SpecificRecord> implements Serde<T> {

  private Serde<T> inner;
  private Function<R, T> sss; // todo compare reflection technique with lambda

  public WrappingSpecificAvroSerde() {
    // TODO configure dynamically
    List<Class> aClass = List.of(InstrumentTickBD.class, HighLowBD.class, GoalEventWrapped.class);

    SpecificAvroSerializer<T> tSpecificAvroSerializer = new SpecificAvroSerializer<>();
    GenerationGapAvroDeserializer<R, T> rtGenerationGapAvroDeserializer = new GenerationGapAvroDeserializer<>(aClass);
    inner = Serdes.serdeFrom(tSpecificAvroSerializer, rtGenerationGapAvroDeserializer);
  }

  public WrappingSpecificAvroSerde(List<Class> clazzes) {
    SpecificAvroSerializer<T> tSpecificAvroSerializer = new SpecificAvroSerializer<>();
    GenerationGapAvroDeserializer<R, T> rtGenerationGapAvroDeserializer = new GenerationGapAvroDeserializer<R, T>(clazzes);
    inner = Serdes.serdeFrom(tSpecificAvroSerializer, rtGenerationGapAvroDeserializer);
  }

  /**
   * For testing purposes only.
   */
  public WrappingSpecificAvroSerde(final SchemaRegistryClient client) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
            new SpecificAvroSerializer<T>(client),
            new GenerationGapAvroDeserializer<R, T>(client));
  }

  @Override
  public Serializer<T> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<T> deserializer() {
    Deserializer<T> deserializer = inner.deserializer();
    return deserializer;
  }

  @Override
  public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
    inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
    inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}
