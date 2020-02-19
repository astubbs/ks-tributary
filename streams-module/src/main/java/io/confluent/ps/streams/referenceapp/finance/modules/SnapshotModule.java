package io.confluent.ps.streams.referenceapp.finance.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.confluent.ps.streams.referenceapp.chat.ChatKpiTopology;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.schools.topology.SchoolTopology;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.WrappingSpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.finance.topologies.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static org.apache.kafka.common.utils.Utils.*;

@Slf4j
public class SnapshotModule extends AbstractModule {

  @Inject
  SnapshotTopologySettings settings;

  @Inject
  KSUtils ksUtils;

  public static final Properties config = mkProperties(mkMap(
          mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestTopicsTest-" + Math.random()),
          mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
          mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once"),
          mkEntry(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KSUtils.MOCK_SCHEMA_REGISTRY_URL),
          mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName()),
          mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WrappingSpecificAvroSerde.class.getName()),
          mkEntry(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")) // disable for debugging
  );

  @Override
  protected void configure() {
    bind(StreamsBuilder.class).in(SINGLETON);
    bind(SnapshotSetsConfigTopology.class).in(SINGLETON);
    bind(SnapshotTopologyLatestWindows.class).in(SINGLETON);
    bind(SnapshotTopologyPrecomputerSimple.class).in(SINGLETON);
    bind(SnapshotTopologyPrecomputerSharded.class).in(SINGLETON);
    bind(SnapshotTopologyHighLowBarWindows.class).in(SINGLETON);
    bind(ChatKpiTopology.class).in(SINGLETON);
    bind(SchoolTopology.class).in(SINGLETON);
  }

  // depends on all topologies having been constructed
  // TODO remove, this smells
  @Provides
  Topology builder(StreamsBuilder builder,
                   SnapshotSetsConfigTopology depOne,
                   SnapshotTopologyLatestWindows depTwo,
                   SnapshotTopologyPrecomputerSimple depThree,
                   SnapshotTopologyPrecomputerSharded depFour,
                   SnapshotTopologyHighLowBarWindows depFive,
                   ChatKpiTopology depSix,
                   SchoolTopology depSeven

  ) {
    Topology build = builder.build();
    return build;
  }

  /**
   * TODO too complex for module - move into service class?
   * <p>
   * Need to specify explicit serdes to ensure {@link KTable#suppress} gets correct Window Serde wrapping behaviour.
   *
   * @return Shared bootstrap topic (can only register input nodes once)
   *
   * @see <a href="https://issues.apache.org/jira/browse/KAFKA-9259">KAFKA-9259</a>
   * @see <a href="https://issues.apache.org/jira/browse/KAFKA-9260">KAFKA-9260</a>
   */
  @Singleton
  @Provides
  KStream<InstrumentId, InstrumentTickBD> instrumentStream(StreamsBuilder builder, KSUtils ksUtils) {
    SpecificAvroSerde<InstrumentId> keySerde = ksUtils.serdeFor(true);
    WrappingSpecificAvroSerde<InstrumentTickBD, InstrumentTickBD> valueSerde = ksUtils.wrappingSerdeFor(true);
    Consumed<InstrumentId, InstrumentTickBD> with = Consumed.with(keySerde, valueSerde);
    KStream<InstrumentId, InstrumentTickBD> stream = builder.stream(SnapshotTopologyParent.INSTRUMENTS_TOPIC_NAME, with);
    return stream;
  }
}
