package io.confluent.ps.streams.referenceapp.finance.services;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyLatestWindows;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link SnapshotTopologyLatestWindows}
 */
//@RequiredArgsConstructor(onConstructor_= {@Inject})
@Slf4j
public class LatestInstrumentWindowsService implements SnapshotSetAggregateSupplier {

  private final SnapshotTopologySettings settings;

  private final SnapshotStoreProvider stores;

  private final SnapshotSetsConfigService snapshotSetsConfigService;

  @Inject
  public LatestInstrumentWindowsService(SnapshotTopologySettings settings, SnapshotStoreProvider stores, SnapshotSetsConfigService snapshotSetsConfigService) {
    this.settings = settings;
    this.stores = stores;
    this.snapshotSetsConfigService = snapshotSetsConfigService;
  }

  private WindowStore<InstrumentId, ValueAndTimestamp<InstrumentTickBD>> getLatestInstrumentWindowsStore() {
    return stores.latestInstrumentWindowsStore();
  }

  private Instant calculateWindowStartTime(Instant windowEndTime) {
    return windowEndTime.minus(settings.getWindowSize());
  }

  public Optional<ValueAndTimestamp<InstrumentTickBD>> findTickForThisInstrumentForWindowEndingAt(InstrumentId key, Instant windowEndTime) {
    Instant windowStartTime = calculateWindowStartTime(windowEndTime);
    WindowStoreIterator<ValueAndTimestamp<InstrumentTickBD>> fetch = getLatestInstrumentWindowsStore().fetch(key, windowStartTime, windowStartTime);
    if (fetch.hasNext()) return Optional.of(Iterators.getOnlyElement(fetch).value);
    else return Optional.empty();
  }

  /**
   * Not precomputed. Converts to list first, so is potentially slow. Can reimplement and return iterator instead.
   *
   * @return
   */
  public ArrayList<KeyValue<InstrumentId, InstrumentTickBD>> findTicksForAllInstrumentWindowsEndingAt(Instant windowEndTime) {
    Instant startTime = windowEndTime.minus(settings.getWindowSize());
    KeyValueIterator<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>> windowedValueAndTimestampKeyValueIterator = getLatestInstrumentWindowsStore().fetchAll(startTime, startTime);
    ArrayList<KeyValue<InstrumentId, InstrumentTickBD>> allLatestInstruments = Lists.newArrayList();
    // unwrap
    windowedValueAndTimestampKeyValueIterator.forEachRemaining(windowedKeyValue -> {
      // for each key, in each found window
      Windowed<InstrumentId> windowedKey = windowedKeyValue.key;
      allLatestInstruments.add(KeyValue.pair(windowedKey.key(), windowedKeyValue.value.value()));
    });
    return allLatestInstruments;
  }

  /**
   * Potentially slow, as not precomputed. One query per key.
   */
  public List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> findTheseTickWindowsEndingAt(List<InstrumentId> instrumentIds, Instant snapTime) {
    List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> result = Lists.newArrayListWithCapacity(instrumentIds.size());
    instrumentIds.forEach(instrumentId -> {
      Optional<ValueAndTimestamp<InstrumentTickBD>> snapForSingleKeyAt = findTickForThisInstrumentForWindowEndingAt(instrumentId, snapTime);
      result.add(KeyValue.pair(instrumentId, snapForSingleKeyAt));
    });
    return result;
  }

  /**
   * Potentially slow, as not precomputed. One query per key.
   *
   * @return
   */
  public List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> findAndCalculateSnapAt(SnapshotSetId snapshotSetId, Instant snapTime) {
    List<InstrumentId> snapKeys = snapshotSetsConfigService.getKeySetForSnap(snapshotSetId);
    List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> result = findTheseTickWindowsEndingAt(snapKeys, snapTime);
    return result;
  }

  /**
   * Artificially construct a {@link SnapshotSetAggregation} at runtime, from one off queries.
   *
   * @param snapshotSetId
   * @param snapTime
   *
   * @return
   */
  public Optional<SnapshotSetAggregation> findAndConstructSnapSetAggregationAt(SnapshotSetId snapshotSetId, Instant snapTime) {
    List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> snapSetAt = this.findAndCalculateSnapAt(snapshotSetId, snapTime);
    Map<String, String> instruments = Maps.newHashMap();
    snapSetAt.forEach(x -> {
      // skip if value is missing for this key (we may not have received ticks for all keys in this snap set)
      if (x.value.isPresent()) {
        String price = x.value.get().value().getPrice();
        instruments.put(x.key.getId(), price);
      }
    });
    return Optional.of(SnapshotSetAggregation.newBuilder().setId(snapshotSetId).setInstruments(instruments).build());
  }

  @Override
  public Optional<SnapshotSetAggregation> findSnapSetAtWindowEndTime(SnapshotSetId snapshotSetId, Instant snapTime) {
    return findAndConstructSnapSetAggregationAt(snapshotSetId, snapTime);
  }

}
