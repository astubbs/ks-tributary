package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.*;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

/**
 * API for accessing state stores at runtime, used for IOC/DI
 */
public interface SnapshotStoreProvider {

  KeyValueStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>> configStore();

  KeyValueStore<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>> inverseConfigStore();

  WindowStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetAggregation>> precomputedSimple();

  WindowStore<SnapshotSetId, SnapshotSetAggregation> shardedPrecomputedSnapsStore();

  WindowStore<InstrumentId, ValueAndTimestamp<InstrumentTickBD>> latestInstrumentWindowsStore();

  WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsOneMinuteStore();

  WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsTenMinuteStore();
}
