package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.GenUtils;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.WallClockStub;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Random;

/**
 * Deterministic, reasonable data
 */
@Singleton
public class TickGenerator {

  Random deterministicIds = new Random(GenUtils.randomSeed);

  Random deterministicPrices = new Random(GenUtils.randomSeed);

  GenUtils genUtils = new GenUtils();

  @Inject
  InstrumentGenerator ig;

  @Inject
  WallClockStub clock;

  Duration by = Duration.ofMillis(1);

  public List<InstrumentTick> constructTicks(int howMany, List<InstrumentId> ids) {
    List<InstrumentTick> someStuff = genUtils.createSomeStuff(howMany, ($) -> {
      InstrumentId id = getWithModulo(ids, deterministicIds.nextInt());
      Integer price = Math.abs(deterministicPrices.nextInt(1000));
      Long tickTime = clock.advanceAndGet(by).toEpochMilli();
      InstrumentTickBD build = InstrumentTickBD.of(id, price.toString(), tickTime);
      return build;
    });
    return someStuff;
  }

  <T> T getWithModulo(List<T> list, int i) {
    int abs = Math.abs(i);
    int size = list.size();
    int mod = (abs % size);
    return list.get(mod);
  }

}
