package io.confluent.ps.streams.referenceapp.utils;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.HighLowBD;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyParent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.WrappingSpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.inject.Inject;

@Singleton
public class KSUtils {

  static private final String SCHEMA_REGISTRY_SCOPE = SnapshotTopologyParent.class.getName();
  public static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
  final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

  @Inject
  public KSUtils() {
  }

  public <T extends SpecificRecord, R extends T> WrappingSpecificAvroSerde<R, T> wrappingSerdeFor(boolean isKey) {
    Function<InstrumentTick, InstrumentTickBD> ss = (x) -> {
      InstrumentTickBD of = InstrumentTickBD.of(x);
      return of;
    };

    List<Class> aClass = List.of(InstrumentTickBD.class, HighLowBD.class);
    WrappingSpecificAvroSerde<R, T> serde1 = new WrappingSpecificAvroSerde<R, T>(aClass);
    serde1.configure(serdeConfig, isKey);

    return serde1;
  }

  public <T extends SpecificRecord> SpecificAvroSerde<T> serdeFor() {
    return this.<T>serdeFor(false);
  }

  public <T extends SpecificRecord> SpecificAvroSerde<T> serdeFor(boolean isKey) {
    SpecificAvroSerde<T> tSpecifivAvroSerde = new SpecificAvroSerde<T>();
    tSpecifivAvroSerde.configure(serdeConfig, isKey);
    return tSpecifivAvroSerde;
  }

}
