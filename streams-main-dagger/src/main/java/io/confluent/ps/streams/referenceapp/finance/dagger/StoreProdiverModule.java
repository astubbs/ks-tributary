package io.confluent.ps.streams.referenceapp.finance.dagger;

import io.confluent.ps.streams.referenceapp.finance.topologies.KSSnapshotStoreProvider;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import dagger.Module;
import dagger.Provides;

@Module
public class StoreProdiverModule {

  @Provides
  SnapshotStoreProvider stores() {
    return new KSSnapshotStoreProvider();
  }

}
