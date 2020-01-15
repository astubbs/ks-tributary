package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestRecord;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetKeys;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotSetsConfigTopology;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyParent;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import dagger.Lazy;
import io.confluent.kafka.streams.serdes.avro.WrappingSpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;

public class TestDataDriver {

  // topics
  protected TestInputTopic<SnapshotSetId, SnapshotSetKeys> configTopic;
  protected TestInputTopic<InstrumentId, InstrumentTickBD> tickTopic;
  protected TestOutputTopic<Windowed<InstrumentId>, InstrumentTickBD> outputTopic;

  TestData td;

  @Inject
  public TestDataDriver(SnapshotTopologySettings settings, KSUtils ksutils, Lazy<TopologyTestDriver> testDriver, TestData td) {
    this.td = td;
    td.baseTimeMinusWindowSize = td.baseTime.minus(settings.getWindowSize()); // i.e. the first window that will have closed given our base time

    // test topics
    SpecificAvroSerde<SnapshotSetId> snapSetIdForKeysSerde = ksutils.serdeFor(true);
    SpecificAvroSerde<InstrumentId> instrumentIdForKeysSerde = ksutils.serdeFor(true);
//    MySpecificAvroSerde<SnapSetKeys, SnapSetKeys> snapSetKeysForValuesSerde = ksutils.<SnapSetKeys, SnapSetKeys>serdeForMy(false);
    SpecificAvroSerde<SnapshotSetKeys> snapSetKeysForValuesSerde = ksutils.serdeFor(false);
    WrappingSpecificAvroSerde<InstrumentTickBD, InstrumentTick> instrumentTick = ksutils.<InstrumentTick, InstrumentTickBD>wrappingSerdeFor(false);

    Serializer<InstrumentId> serializer = instrumentIdForKeysSerde.serializer();
    Serializer<InstrumentTickBD> serializer1 = instrumentTick.serializer();
    tickTopic = testDriver.get().<InstrumentId, InstrumentTickBD>createInputTopic(SnapshotTopologyParent.INSTRUMENTS_TOPIC_NAME, serializer, serializer1);
    configTopic = testDriver.get().createInputTopic(SnapshotSetsConfigTopology.SNAP_CONFIGS_TOPIC, snapSetIdForKeysSerde.serializer(), snapSetKeysForValuesSerde.serializer());

    WindowedSerdes.TimeWindowedSerde<InstrumentId> windowedSerde = new WindowedSerdes.TimeWindowedSerde<>(ksutils.serdeFor(true));
    outputTopic = testDriver.get().createOutputTopic(SnapshotTopologyParent.FINAL_SNAP_VALUES_TOPIC_NAME, windowedSerde.deserializer(), instrumentTick.deserializer());
  }

  protected void insertConfigsData() {
    configTopic.pipeInput(td.snapSetConfigOne.getId(), td.snapSetConfigOne, td.firstTickTimeStamp);
    configTopic.pipeInput(td.snapSetConfigTwo.getId(), td.snapSetConfigTwo, td.firstTickTimeStamp);
  }

  public TestRecord<InstrumentId, InstrumentTickBD> tickTopicPipe(String id, String price, Instant recordTime) {
    TestRecord<InstrumentId, InstrumentTickBD> value = tickFor(id, price, recordTime);
    tickTopicPipe(value);
    return value;
  }

  protected void tickTopicPipe(String id, Integer price, Instant recordTime) {
    tickTopicPipe(id, price.toString(), recordTime);
  }

  protected void tickTopicPipe(TestRecord<InstrumentId, InstrumentTickBD> value) {
    tickTopic.pipeInput(value);
  }

  protected TestRecord<InstrumentId, InstrumentTickBD> tickFor(String id, Integer price, Instant stamp) {
    return this.tickFor(id, price.toString(), stamp);
  }

  protected TestRecord<InstrumentId, InstrumentTickBD> tickFor(String id, String price, Instant stamp) {
    // use same time stamp for bloomburg time and record time
    //InstrumentTick tick = InstrumentTick.newBuilder().setId(new InstrumentId(id)).setPrice(price).setBloombergTimestamp(stamp.toEpochMilli()).build();
    InstrumentTickBD tick = InstrumentTickBD.of(new InstrumentId(id), price, stamp);
    InstrumentId key = InstrumentId.newBuilder().setId(id).build();
    TestRecord<InstrumentId, InstrumentTickBD> testRecord = new TestRecord<InstrumentId, InstrumentTickBD>(key, tick, stamp);
    return testRecord;
  }

  public void insertTestDataSetOne() {
    tickTopic.pipeInput(tickFor(td.appleInstrumentOne, td.firstTick20, td.firstTickTimeStamp));
    tickTopic.pipeInput(tickFor(td.appleInstrumentOne, td.secondTick50, baseTimePlusHours(1)));
    tickTopic.pipeInput(tickFor(td.appleInstrumentOne, td.thirdTick10, baseTimePlusHours(3)));
  }

  protected void insertTestDataSetTwo() {
    tickTopic.pipeInput(tickFor(td.googleInstrumentTwo, "120", td.firstTickTimeStamp));
    tickTopic.pipeInput(tickFor(td.googleInstrumentTwo, "150", baseTimePlusHours(1)));
    tickTopic.pipeInput(tickFor(td.googleInstrumentTwo, "130", baseTimePlusHours(3)));
  }

  protected void insertTestDataSetThree() {
    tickTopic.pipeInput(tickFor(td.starbucksInstrumentThreeId.getId(), "400", baseTimePlusHours(1)));
    tickTopic.pipeInput(tickFor(td.starbucksInstrumentThreeId.getId(), td.instrumentThreeTickTwo400, baseTimePlusHours(3)));
  }

  public void insertAllTestDataInOrder() {
    insertEarlierTestDataInOrder();

    insertLaterTestDataInOrder();
  }

  void insertEarlierTestDataInOrder() {
    tickTopicPipe(td.appleInstrumentOne, td.firstTick20, td.firstTickTimeStamp);
    tickTopicPipe(td.googleInstrumentTwo, "120", td.firstTickTimeStamp);

    tickTopicPipe(td.appleInstrumentOne, td.secondTick50, baseTimePlusHours(1));
    tickTopicPipe(td.googleInstrumentTwo, "150", baseTimePlusHours(1));
    tickTopicPipe(td.starbucksInstrumentThreeId.getId(), td.instrumentThreeTickOne490, baseTimePlusHours(1));
  }


  @Getter
  final List<TestRecord<InstrumentId, InstrumentTickBD>> laterDataStream = Lists.newArrayList();

  void insertLaterTestDataInOrder() {
    laterDataStream.add(tickFor(td.appleInstrumentOne, td.thirdTick10, baseTimePlusHours(3)));
    laterDataStream.add(tickFor(td.googleInstrumentTwo, "130", baseTimePlusHours(3)));
    laterDataStream.add(tickFor(td.starbucksInstrumentThreeId.getId(), td.instrumentThreeTickTwo400, baseTimePlusHours(3)));

    pipeOutput(laterDataStream);
  }

  @Getter
  final List<TestRecord<InstrumentId, InstrumentTickBD>> highFrequencyDataStream = Lists.newArrayList();

  protected void insertTestDataSetOneHighFrequencyInOrder() {
    Instant base = td.firstTickTimeStamp;

    // 1 minute window
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, td.firstTick20, base));
    highFrequencyDataStream.add(tickFor(td.googleInstrumentTwo, "120", base)); // low
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, td.secondTick50, base.plusSeconds(20)));
    highFrequencyDataStream.add(tickFor(td.googleInstrumentTwo, "150", base.plusSeconds(22))); // high
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, td.thirdTick10, base.plusSeconds(70)));
    highFrequencyDataStream.add(tickFor(td.googleInstrumentTwo, "130", base.plusSeconds(72)));

    // 10 minute window
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, "120", base.plus(ofMinutes(2))));
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, "150", base.plus(ofMinutes(3))));
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, "250", base.plus(ofMinutes(4))));
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, "1", base.plus(ofMinutes(5))));

    //
    highFrequencyDataStream.add(tickFor(td.appleInstrumentOne, td.thirdTick10 + 1000, base.plus(ofMinutes(11))));

    pipeOutput(highFrequencyDataStream);
  }

  private void pipeOutput(List<TestRecord<InstrumentId, InstrumentTickBD>> dataStream) {
    dataStream.forEach(x->tickTopic.pipeInput(x));
  }

  /**
   * advance stream time to close windows
   *
   * @param hours
   */
  private void advanceTime(int hours) {
    Instant endTime = baseTimePlusHours(hours);
    tickTopic.pipeInput(tickFor("", td.dummyTickValue, endTime));
  }

  private Instant baseTimePlusDuration(Duration hours) {
    return td.baseTime.plus(hours);
  }

  protected Instant baseTimePlusHours(int hours) {
    return baseTimePlusDuration(ofHours(hours));
  }

  protected List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> readOutputNonSynthetic() {
    return outputTopic.readKeyValuesToList()
            .stream()
            .filter(it -> {
              InstrumentId key = it.key.key();
              return !key.getId().equals(td.SYNTHETIC_KEY);
            }).collect(Collectors.toList());
  }

  private List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> readOutputIncludingSynthetic() {
    return outputTopic.readKeyValuesToList();
  }

  protected void advanceStreamTimeTo(Instant advanceTo) {
    tickTopic.pipeInput(tickFor(td.SYNTHETIC_KEY, "88", advanceTo));
  }

  protected void advanceStreamTimeBy(Duration advanceBy) {
    Instant timestamp = td.baseTime.plus(advanceBy);
    tickTopic.pipeInput(tickFor(td.SYNTHETIC_KEY, "88", timestamp));
  }
}
