package io.confluent.ps.streams.referenceapp.denormilsation;

import com.google.inject.AbstractModule;
import io.confluent.ps.streams.referenceapp.denormilsation.topology.StoreProvider;

public class DenormalisationTestModule extends AbstractModule {

  @Override
  protected void configure() {
    super.bind(StoreProvider.class).to(TTDStoreProvider.class);
  }

}
