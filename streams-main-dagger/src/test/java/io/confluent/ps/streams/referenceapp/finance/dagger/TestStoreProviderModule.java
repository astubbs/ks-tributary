package io.confluent.ps.streams.referenceapp.finance.dagger;

import dagger.Binds;
import dagger.Module;
import io.confluent.ps.streams.referenceapp.finance.TestStoresProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;

@Module
public abstract class TestStoreProviderModule {

  @Binds abstract SnapshotStoreProvider storeProvider(TestStoresProvider test);

  // TODO When this module needs to actually be used, will need to fix the
  // bindings below
  //  @Provides
  //  @Singleton
  //  static Lazy<TopologyTestDriverProvider>
  //  lazyDriverProvider(TopologyTestDriver t) {
  //    return new TopologyTestDriverProvider() {
  //
  //      @Override
  //      public TopologyTestDriver getInstance(Class c) {
  //        return t;
  //      }
  //    };
  //  }
  //
  //  @Provides
  //  @Singleton
  //  static TopologyTestDriverProvider driverProvider(TopologyTestDriver t) {
  //    return new TopologyTestDriverProvider() {
  //
  //      @Override
  //      public TopologyTestDriver getInstance(Class c) {
  //        return t;
  //      }
  //    };
  //  }
}
