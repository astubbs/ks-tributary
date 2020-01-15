package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsConfigService;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import dagger.Lazy;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.inject.Inject;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestModuleStoresTest {

  @Inject
  protected SnapshotStoreProvider stores;

  @Inject
  Lazy<TopologyTestDriver> testDriver;

  @TempDir
  Path tempDir;

  @Test
  void getStores() {
    Injector injector = Guice.createInjector(new TestModule(tempDir));

    injector.injectMembers(this);

    Map<String, StateStore> allStateStores = injector.getInstance(TopologyTestDriver.class).getAllStateStores();
    assertThat(allStateStores.entrySet()).as("all state stores are actually pressent").hasSize(8);

    assertThat(stores.configStore()).isNotNull();
    assertThat(stores.inverseConfigStore()).isNotNull();
    assertThat(stores.precomputedSimple()).isNotNull();
    assertThat(stores.shardedPrecomputedSnapsStore()).isNotNull();
    assertThat(stores.latestInstrumentWindowsStore()).isNotNull();
    assertThat(stores.highLowBarsOneMinuteStore()).isNotNull();
    assertThat(stores.highLowBarsTenMinuteStore()).isNotNull();

    assertThat(injector.getBinding(SnapshotSetsConfigService.class)).isNotNull();
  }
}
