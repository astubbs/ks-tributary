package io.confluent.ps.streams.referenceapp.finance.services;

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Optional;

public class SnapshotSetsPrecomputedSimpleService implements SnapshotSetAggregateSupplier {

  @Inject
  protected SnapshotStoreProvider stores;

  @Inject
  SnapshotTopologySettings settings;

  @Inject
  public SnapshotSetsPrecomputedSimpleService(SnapshotStoreProvider stores, SnapshotTopologySettings settings) {
    this.stores = stores;
    this.settings = settings;
  }

  public Optional<SnapshotSetAggregation> simpleFetchSnapAt(SnapshotSetId id, Instant snapTime) {
    Instant windowStart = snapTime.minus(settings.getWindowSize());
    WindowStoreIterator<ValueAndTimestamp<SnapshotSetAggregation>> fetch = stores.precomputedSimple().fetch(id, windowStart, windowStart);
    Optional<KeyValue<Long, ValueAndTimestamp<SnapshotSetAggregation>>> first = ImmutableList.copyOf(fetch).stream().findFirst();
    return first.map(it -> it.value.value());
  }

  @Override
  public Optional<SnapshotSetAggregation> findSnapSetAtWindowEndTime(SnapshotSetId snapshotSetId, Instant snapTime) {
    return simpleFetchSnapAt(snapshotSetId, snapTime);
  }
}
