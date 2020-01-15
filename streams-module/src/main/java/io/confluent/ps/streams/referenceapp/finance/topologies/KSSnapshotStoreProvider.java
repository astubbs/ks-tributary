package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.*;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;

public class KSSnapshotStoreProvider implements SnapshotStoreProvider {

  @Inject
  public KSSnapshotStoreProvider() {
  }

  @Override
  public KeyValueStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>> configStore() {
    return null;
  }

  @Override
  public KeyValueStore<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>> inverseConfigStore() {
    return null;
  }

  @Override
  public WindowStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetAggregation>> precomputedSimple() {
    return null;
  }

  @Override
  public WindowStore<SnapshotSetId, SnapshotSetAggregation> shardedPrecomputedSnapsStore() {
    return null;
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<InstrumentTickBD>> latestInstrumentWindowsStore() {
    return null;
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsOneMinuteStore() {
    return null;
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsTenMinuteStore() {
    return null;
  }
}
