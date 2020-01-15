package io.confluent.ps.streams.referenceapp.finance.services;

import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentsToSnapshotSets;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetKeys;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import lombok.NoArgsConstructor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

@NoArgsConstructor
public class SnapshotSetsConfigService {

  @Inject
  SnapshotStoreProvider stores;

  @Inject
  public SnapshotSetsConfigService(SnapshotStoreProvider stores) {
    this.stores = stores;
  }

  public List<InstrumentId> getKeySetForSnap(SnapshotSetId snapshotSetId) {
    Optional<ValueAndTimestamp<SnapshotSetKeys>> snapSetKeys = Optional.ofNullable(stores.configStore().get(snapshotSetId));
    return snapSetKeys.get().value().getInstruments();
  }

  /**
   * Get all the snap sets which reference this key
   *
   * @param readOnlyKey
   * @return
   */
  public Optional<InstrumentsToSnapshotSets> findAllMatchingSnapsetsForKeyFastInverseTechnique(InstrumentId readOnlyKey) {
    ValueAndTimestamp<InstrumentsToSnapshotSets> instrumentsToSnapSets = stores.inverseConfigStore().get(readOnlyKey);
    if (instrumentsToSnapSets == null)
      return Optional.empty();
    return Optional.of(instrumentsToSnapSets.value());
  }

  /**
   * Get all the snapshots sets which reference this key
   *
   * @param readOnlyKey
   * @return
   */
  public List<SnapshotSetId> findAllMatchingSnapsetsForKeySlowScanTechnique(InstrumentId readOnlyKey) {
    List<SnapshotSetId> result = Lists.newArrayList();
    stores.configStore().all().forEachRemaining(config -> {
      if (config.value.value().getInstruments().contains(readOnlyKey)) {
        SnapshotSetId key = config.key;
        result.add(key);
      }
    });
    return result;
  }

  public KeyValueIterator<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>> getAllConfigs() {
    KeyValueIterator<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>> all = stores.configStore().all();
    return all;
  }

  public KeyValueIterator<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>> getAllInverseConfigs() {
    KeyValueIterator<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>> all = stores.inverseConfigStore().all();
    return all;
  }

}
