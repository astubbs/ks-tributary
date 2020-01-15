package io.confluent.ps.streams.referenceapp.finance.topologies;

import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentsToSnapshotSets;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetKeys;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import javax.inject.Inject;
import java.util.List;

/**
 * Dynamic configuration - the Snapshot Set configs get pushed into a topic, and the app reads them directly on instance
 */
public class SnapshotSetsConfigTopology {

  public static final String SNAP_CONFIGS_TOPIC = "snap-configs";
  public static final String SNAP_CONFIGS_STORE = "snap-configs-store";
  public static final String SNAP_CONFIGS_INVERSE_STORE = "snap-configs-inverse-store";

  @Inject
  public SnapshotSetsConfigTopology(StreamsBuilder builder) {
    // straight up save all the config sets
    KTable<SnapshotSetId, SnapshotSetKeys> table = builder.table(SNAP_CONFIGS_TOPIC, Materialized.as(SNAP_CONFIGS_STORE));

    // you can't register two sources, so reuse the table to build the stream (the other way around is more work because we have to specify the reduce method
    KStream<SnapshotSetId, SnapshotSetKeys> stream = table.toStream();

    // invert the stream, so we can have a config stream of instruments to snapsets
    KStream<InstrumentId, SnapshotSetId> instrumentIdSnapSetIdKStream = stream.flatMap((snapSetId, value) -> {
      List<KeyValue<InstrumentId, SnapshotSetId>> keys = Lists.newArrayList();
      value.getInstruments().forEach(instrumentId ->
      {
        keys.add(KeyValue.pair(instrumentId, snapSetId));
      });
      return keys;
    });

    // collect into queryable table of instrument id -> [SnapSetIds]
    instrumentIdSnapSetIdKStream.groupByKey().aggregate(() -> {
        InstrumentId dummyKey = InstrumentId.newBuilder().setId("").build();
      return InstrumentsToSnapshotSets.newBuilder().setId(dummyKey).build();
            }, (key, value, aggregate) -> {
              // key isn't available in the init phase, so we have to set it, or make a new object type that
              // omits it (KS cant do straight lists or maps)
              // KAFKA-8326 - Add Serde<List<Inner>> support
              // TODO could just remove the key from the value - it's always in the key
              aggregate.setId(key);
              aggregate.getSnapSets().add(value);
              return aggregate;
            }, Materialized.as(SNAP_CONFIGS_INVERSE_STORE));
  }
}
