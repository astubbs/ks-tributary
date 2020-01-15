package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.google.common.collect.Maps;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.GenUtils;
import lombok.extern.slf4j.Slf4j;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import javax.inject.Singleton;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Deterministic, reasonable data
 */
@Singleton
@Slf4j
public class InstrumentGenerator {

  PodamFactory factory = new PodamFactoryImpl();

  GenUtils genUtils = new GenUtils();

  Locale nl = Locale.forLanguageTag("nl");
  Name wordStream;

  Faker faker = new Faker(nl, new Random(GenUtils.randomSeed));

  public InstrumentGenerator() {
    wordStream = faker.name();
  }

  Map wordStreamCache = Maps.newHashMap();

  int duplicateCount = 0;

  // TODO increase 20000 - there are about 23,000 data points in the source java faker data
  static Integer INSTRUMENT_COUNT = 2000;

  public List<InstrumentId> constructInstruments() {
//    await().untilAsserted(() -> {
      List<InstrumentId> someStuff = genUtils.createSomeStuff(INSTRUMENT_COUNT, ($) -> {
        String word = wordStream.lastName();
        // https://github.com/DiUS/java-faker/issues/463 :(
        while (wordStreamCache.containsKey(word)) {
          duplicateCount++;
          log.trace("Duplicate: " + word);
          word = wordStream.lastName();
        }
        wordStreamCache.put(word, 0);
        return InstrumentId.newBuilder().setId(word).build();
      });
//      assertThat(someStuff).hasSizeGreaterThan(INSTRUMENT_COUNT);
//    });
    log.info("Dupes: " + duplicateCount);
    System.out.println("Dupes: " + duplicateCount);
    return someStuff;
  }

}
