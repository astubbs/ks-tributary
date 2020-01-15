package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.GenUtils;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.WallClockStub;

import java.util.List;

public class DataGenModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(WallClockStub.class).toInstance(new WallClockStub(GenUtils.randomSeedInstant));
  }

  @Provides
  List<InstrumentId> ids(InstrumentGenerator gen) {
    return gen.constructInstruments();
  }
}
