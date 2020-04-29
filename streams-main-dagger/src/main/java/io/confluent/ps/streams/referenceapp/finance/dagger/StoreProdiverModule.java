package io.confluent.ps.streams.referenceapp.finance.dagger;

import dagger.Module;
import dagger.Provides;
import io.confluent.ps.streams.referenceapp.finance.topologies.KSSnapshotStoreProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;

@Module
public class StoreProdiverModule {

  @Provides
  SnapshotStoreProvider stores() {
    return new KSSnapshotStoreProvider();
  }
}
