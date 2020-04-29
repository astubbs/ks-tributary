package io.confluent.ps.streams.referenceapp.finance.dagger;

import dagger.Module;
import dagger.Provides;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.WrappingSpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.modules.SnapshotModule;
import io.confluent.ps.streams.referenceapp.finance.topologies.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import java.util.Properties;
import javax.inject.Singleton;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

//@NoArgsConstructor
@Module
public class SnapshotDaggerModule {

  // depends on all topologies having been constructed
  @Provides
  Topology topology(StreamsBuilder builder, SnapshotSetsConfigTopology depOne,
                    SnapshotTopologyLatestWindows depTwo,
                    SnapshotTopologyPrecomputerSimple depThree,
                    SnapshotTopologyPrecomputerSharded depFour) {
    return builder.build();
  }

  @Singleton
  @Provides
  StreamsBuilder builder() {
    return new StreamsBuilder();
  }

  @Provides
  Properties config() {
    return SnapshotModule.config;
  }

  /**
   * TODO too complex for module - move into service class?
   * <p>
   * Need to specify explicit serdes to ensure {@link KTable#suppress} gets
   * correct Window Serde wrapping behaviour.
   *
   * @return Shared bootstrap topic (can only register input nodes once)
   *
   * @see <a
   *     href="https://issues.apache.org/jira/browse/KAFKA-9259">KAFKA-9259</a>
   * @see <a
   *     href="https://issues.apache.org/jira/browse/KAFKA-9260">KAFKA-9260</a>
   */
  @Singleton
  @Provides
  KStream<InstrumentId, InstrumentTickBD>
  instrumentStream(StreamsBuilder builder, KSUtils ksUtils) {
    SpecificAvroSerde<InstrumentId> keySerde = ksUtils.serdeFor(true);
    WrappingSpecificAvroSerde<InstrumentTickBD, InstrumentTickBD> valueSerde =
        ksUtils.wrappingSerdeFor(true);
    Consumed<InstrumentId, InstrumentTickBD> with =
        Consumed.with(keySerde, valueSerde);
    KStream<InstrumentId, InstrumentTickBD> stream =
        builder.stream(SnapshotTopologyParent.INSTRUMENTS_TOPIC_NAME, with);
    return stream;
  }
}
