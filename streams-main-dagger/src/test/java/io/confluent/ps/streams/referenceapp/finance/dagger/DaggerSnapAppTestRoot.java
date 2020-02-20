package io.confluent.ps.streams.referenceapp.finance.dagger;

import io.confluent.ps.streams.referenceapp.finance.SnapshotDaggerApp;
import io.confluent.ps.streams.referenceapp.finance.TestData;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import dagger.Module;
import dagger.Component;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Module
@Slf4j
public class DaggerSnapAppTestRoot {

  @Singleton
  @Component(modules = {DaggerSnapAppTestModule.class, TestStoreProviderModule.class, SnapshotDaggerModule.class})
  public interface SnapshotAppCompTestComponent extends SnapshotDaggerApp.SnapshotAppComp {
    LatestInstrumentWindowsService latestService();
    TestData testData();
    TestDataDriver testDataDriver();
  }

}
