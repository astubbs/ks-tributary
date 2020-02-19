package io.confluent.ps.streams.referenceapp.school;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.confluent.ps.streams.referenceapp.schools.topology.StoreProvider;

public class SchoolTestModule extends AbstractModule {

  @Override
  protected void configure() {
    super.bind(StoreProvider.class).to(TTDStoreProvider.class);
  }

}
