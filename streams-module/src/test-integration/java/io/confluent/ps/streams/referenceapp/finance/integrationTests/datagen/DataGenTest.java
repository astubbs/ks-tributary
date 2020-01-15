package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import io.confluent.ps.streams.referenceapp.finance.TestData;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.GenUtils;
import io.confluent.ps.streams.referenceapp.integrationTests.datagen.WallClockStub;
import name.falgout.jeffrey.testing.junit.guice.GuiceExtension;
import name.falgout.jeffrey.testing.junit.guice.IncludeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

@IncludeModule(DataGenModule.class)
@ExtendWith(GuiceExtension.class)
public class DataGenTest {

  Faker faker = Faker.instance();

  PodamFactory factory = new PodamFactoryImpl();

  InstrumentGenerator idsGen;

  List<InstrumentId> instIds;

  TickGenerator tickGenerator;

  @Inject
  Injector inj;

  @BeforeEach
  void setup() {
    WallClockStub clock = new WallClockStub(GenUtils.randomSeedInstant);
    tickGenerator = new TickGenerator();
    tickGenerator.clock = clock;
    idsGen = new InstrumentGenerator();
    instIds = idsGen.constructInstruments();
  }

  @Test
  void generatesSameTicksDeterministicly(){
    WallClockStub clockOne = new WallClockStub(GenUtils.randomSeedInstant);
    WallClockStub clockTwo = new WallClockStub(GenUtils.randomSeedInstant);
    assertThat(clockOne).isNotSameAs(clockTwo);

    TickGenerator tickGeneratorOne = new TickGenerator();
    tickGeneratorOne.clock = clockOne;
    InstrumentTick tickOne = tickGeneratorOne.constructTicks(1, instIds).stream().findFirst().get();

    TickGenerator tickGeneratorTwo = inj.getInstance(TickGenerator.class);
    tickGeneratorTwo.clock = clockTwo;
    InstrumentTick tickTwo = tickGeneratorTwo.constructTicks(1, instIds).stream().findFirst().get();

    assertThat(tickOne).as("generated the same every time").isEqualTo(tickTwo);
  }

  @Test
  void testGeneratedTicks() {
    //
    assertThat(instIds).hasSize(InstrumentGenerator.INSTRUMENT_COUNT);

    //
    List<InstrumentTick> ticks = tickGenerator.constructTicks(5, instIds);
    assertThat(ticks).hasSize(5).satisfies(x -> {
      assertThat(x).extracting(InstrumentTick::getPrice).allSatisfy(y -> assertThat(y).isGreaterThan("0"));
      assertThat(x).extracting(InstrumentTick::getBloombergTimestamp).satisfies(tt -> assertThat(tt.stream().distinct()).as("unique times").hasSize(5));
      assertThat(x).extracting(InstrumentTick::getBloombergTimestamp).containsOnlyOnce(395107200001L);
    });
  }

  @Test
  void deterministicTicks() {
    int howMany = 500;

    //
    TickGenerator freshResetTickGeneratorOne = getFreshGenerator();
    List<InstrumentTick> tickOne = freshResetTickGeneratorOne.constructTicks(howMany, instIds);

    //
    TickGenerator freshResetTickGeneratorTwo = getFreshGenerator();
    List<InstrumentTick> tickTwo = freshResetTickGeneratorTwo.constructTicks(howMany, instIds);

    //
    assertThat(tickOne).as("The exact same ticks are generated every time").isEqualTo(tickTwo);
  }

  private TickGenerator getFreshGenerator() {
    TickGenerator freshResetTickGenerator = new TickGenerator();
    freshResetTickGenerator.clock = new WallClockStub(GenUtils.randomSeedInstant);
    return freshResetTickGenerator;
  }

  @Test
  void fakerTestRandom() {
    String s = faker.address().buildingNumber();
    String title = faker.book().title();
    String s1 = faker.name().lastName();
    String s2 = faker.address().zipCode();
  }

  @Test
  void fakerRandomDeterministic() {
    Random deterministicRandomSequence = new Random(GenUtils.randomSeed);

    Faker faker = new Faker(deterministicRandomSequence);
    ArrayList<String> strings = Lists.newArrayList(faker.stock().nsdqSymbol(), faker.stock().nsdqSymbol(), faker.stock().nsdqSymbol(), faker.stock().nsdqSymbol());
    assertThat(strings).isEqualTo(Lists.newArrayList("BBBY", "ERII", "JCOM", "PFBI"));
    FakeValuesService fakeValuesService = new FakeValuesService(Locale.ENGLISH, new RandomService(deterministicRandomSequence));
  }

  @Test
  void deterministicNumbers() {
    TestData testData = new TestData(new SnapshotTopologySettings());
    long seed = testData.baseTime.toEpochMilli();
    Random rand = new Random(seed);
    ArrayList<Integer> integers = Lists.newArrayList(rand.ints(5).iterator());
    assertThat(integers).isEqualTo(Lists.newArrayList(1840947622, -149449734, 791062994, -303697371, 842184030));
  }

}
