package io.confluent.ps.streams.referenceapp.finance.services;

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Optional;

public class SnapshotSetsPrecomputedShardedService implements SnapshotSetAggregateSupplier {

  @Inject
  protected SnapshotStoreProvider stores;

  @Inject
  SnapshotTopologySettings settings;

  @Inject
  public SnapshotSetsPrecomputedShardedService(SnapshotStoreProvider stores, SnapshotTopologySettings settings) {
    this.stores = stores;
    this.settings = settings;
  }

  public Optional<SnapshotSetAggregation> shardedFetchSnapAt(SnapshotSetId id, Instant snapTime) {
    Instant windowStart = snapTime.minus(settings.getWindowSize());
    WindowStoreIterator<SnapshotSetAggregation> fetch = stores.shardedPrecomputedSnapsStore().fetch(id, windowStart, windowStart);
    Optional<KeyValue<Long, SnapshotSetAggregation>> first = ImmutableList.copyOf(fetch).stream().findFirst();
    return first.map(it -> it.value);
  }

  @Override
  public Optional<SnapshotSetAggregation> findSnapSetAtWindowEndTime(SnapshotSetId snapshotSetId, Instant snapTime) {
    return shardedFetchSnapAt(snapshotSetId, snapTime);
  }

  public Optional<SnapshotSetAggregation> findSnapSetAtWindowStartTime(SnapshotSetId snapshotSetId, Instant windowStartInstant) {
    return shardedFetchSnapAt(snapshotSetId, windowStartInstant.plus(settings.getWindowSize()));
  }
}

