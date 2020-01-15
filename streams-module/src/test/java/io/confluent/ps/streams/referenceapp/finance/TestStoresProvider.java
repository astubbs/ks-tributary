package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.*;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import dagger.Lazy;
import io.confluent.ps.streams.referenceapp.finance.topologies.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;

//@RequiredArgsConstructor(onConstructor_={@Inject})
@Singleton
@Slf4j
public class TestStoresProvider implements SnapshotStoreProvider {

  Lazy<TopologyTestDriver> injector;

  @Inject
  public TestStoresProvider(Lazy<TopologyTestDriver> injector) {
    this.injector = injector;
  }

  private TopologyTestDriver getTestDriver() {
    TopologyTestDriver topologyTestDriverProvider = injector.get();
//    return topologyTestDriverProvider.getInstance(TopologyTestDriver.class);
    return topologyTestDriverProvider;
//    return injector;
  }

  @Override
  public KeyValueStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>> configStore() {
    return getTestDriver().getTimestampedKeyValueStore(SnapshotSetsConfigTopology.SNAP_CONFIGS_STORE);
  }

  @Override
  public KeyValueStore<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>> inverseConfigStore() {
    return getTestDriver().getTimestampedKeyValueStore(SnapshotSetsConfigTopology.SNAP_CONFIGS_INVERSE_STORE);
  }

  @Override
  public WindowStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetAggregation>> precomputedSimple() {
    WindowStore<SnapshotSetId, ValueAndTimestamp<SnapshotSetAggregation>> timestampedWindowStore = getTestDriver().getTimestampedWindowStore(SnapshotTopologyPrecomputerSimple.PRECOMPUTED_SNAPSET_SIMPLE);
    return Objects.requireNonNull(timestampedWindowStore);
  }

  @Override
  public WindowStore<SnapshotSetId, SnapshotSetAggregation> shardedPrecomputedSnapsStore() {
    WindowStore<SnapshotSetId, SnapshotSetAggregation> windowStore = getTestDriver().getWindowStore(SnapshotTopologyPrecomputerSharded.PRECOMPUTED_SNAPSET_SHARDS);
    return Objects.requireNonNull(windowStore);
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<InstrumentTickBD>> latestInstrumentWindowsStore() {
    WindowStore<InstrumentId, ValueAndTimestamp<InstrumentTickBD>> timestampedWindowStore = getTestDriver().getTimestampedWindowStore(SnapshotTopologyLatestWindows.LATEST_INSTRUMENT_WINDOWS_STORE_NAME);
    return Objects.requireNonNull(timestampedWindowStore);
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsOneMinuteStore() {
    Map<String, StateStore> allStateStores = getTestDriver().getAllStateStores();
    WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> windowStore = getTestDriver().getTimestampedWindowStore(SnapshotTopologyHighLowBarWindows.HIGH_LOW_BARS_ONE_MINUTE_STORE_NAME);
    return Objects.requireNonNull(windowStore);
  }

  @Override
  public WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> highLowBarsTenMinuteStore() {
    WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> windowStore = getTestDriver().getTimestampedWindowStore(SnapshotTopologyHighLowBarWindows.HIGH_LOW_BARS_TEN_MINUTE_STORE_NAME);
    return Objects.requireNonNull(windowStore);
  }

}
