package io.confluent.ps.streams.referenceapp.finance.dagger;

import dagger.Component;
import dagger.Module;
import io.confluent.ps.streams.referenceapp.finance.SnapshotDaggerApp;
import io.confluent.ps.streams.referenceapp.finance.TestData;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Module
@Slf4j
public class DaggerSnapAppTestRoot {

  @Singleton
  @Component(modules = {DaggerSnapAppTestModule.class,
                        TestStoreProviderModule.class,
                        SnapshotDaggerModule.class})
  public interface SnapshotAppCompTestComponent
      extends SnapshotDaggerApp.SnapshotAppComp {
    LatestInstrumentWindowsService latestService();
    TestData testData();
    TestDataDriver testDataDriver();
  }
}
