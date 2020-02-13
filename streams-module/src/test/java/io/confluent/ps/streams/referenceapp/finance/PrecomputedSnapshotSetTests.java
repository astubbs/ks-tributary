package io.confluent.ps.streams.referenceapp.finance;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsPrecomputedShardedService;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsPrecomputedSimpleService;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyPrecomputerSharded;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyPrecomputerSimple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PrecomputedSnapshotSetTests extends DaggerTestBase {

  @Inject
  protected TestData td;

  @Inject
  protected LatestInstrumentWindowsService instrumentService;

  @Inject
  SnapshotSetsPrecomputedShardedService snapSetsPrecomputedShardedService;

  @Inject
  SnapshotSetsPrecomputedSimpleService snapSetsPrecomputedSimpleService;

  @Inject
  SnapshotTopologyPrecomputerSimple snapTopologyPrecomputerSimple;

  @Inject
  SnapshotTopologyPrecomputerSharded snapTopologyPrecomputerSharded;

  @Test
  void testSimpleMethodTwoTicks() {
    tdd.tickTopic.pipeInput(tdd.tickFor(td.appleInstrumentOne, td.firstTick20, td.firstTickTimeStamp));

    SnapshotSetAggregation snapshotSetAggregationOne = snapSetsPrecomputedSimpleService.simpleFetchSnapAt(td.snapSetConfigOne.getId(), td.snapTime).get();
    Assertions.assertThat(snapshotSetAggregationOne.getId()).isEqualTo(td.snapSetConfigOne.getId());
    assertThat(snapshotSetAggregationOne.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.firstTick20);

    //
    tdd.tickTopic.pipeInput(tdd.tickFor(td.appleInstrumentOne, td.secondTick50, tdd.baseTimePlusHours(1)));

    //
    SnapshotSetAggregation snapshotSetAggregationTwo = snapSetsPrecomputedSimpleService.simpleFetchSnapAt(td.snapSetConfigOne.getId(), td.snapTime).get();
    Assertions.assertThat(snapshotSetAggregationTwo.getId()).isEqualTo(td.snapSetConfigOne.getId());
    assertThat(snapshotSetAggregationTwo.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.secondTick50);
  }

  @Test
  void testSimpleMethodInOrderData() {
    tdd.insertAllTestDataInOrder();

    // set one
    SnapshotSetAggregation snapshotSetAggregationOne = snapSetsPrecomputedSimpleService.simpleFetchSnapAt(td.snapSetConfigOne.getId(), td.snapTime).get();
    Assertions.assertThat(snapshotSetAggregationOne.getId()).isEqualTo(td.snapSetConfigOne.getId());
    assertThat(snapshotSetAggregationOne.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.thirdTick10);

    // set two
    SnapshotSetAggregation snapshotSetAggregationTwo = snapSetsPrecomputedSimpleService.simpleFetchSnapAt(td.snapSetConfigTwo.getId(), td.snapTime).get();
    assertThat(snapshotSetAggregationTwo.getInstruments().keySet()).containsOnly(td.appleInstrumentOne, td.starbucksInstrumentThreeId.getId());
    assertThat(snapshotSetAggregationTwo.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.thirdTick10, td.instrumentThreeTickTwo400);
  }

  @Test
  void testSharded() {
    tdd.insertAllTestDataInOrder();

    SnapshotSetAggregation snapshotSetAggregationOne = snapSetsPrecomputedShardedService.shardedFetchSnapAt(td.snapSetConfigOne.getId(), td.snapTime).get();
    Assertions.assertThat(snapshotSetAggregationOne.getId()).isEqualTo(td.snapSetConfigOne.getId());
    assertThat(snapshotSetAggregationOne.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.thirdTick10);

    // set two
    SnapshotSetAggregation snapshotSetAggregationTwo = snapSetsPrecomputedShardedService.shardedFetchSnapAt(td.snapSetConfigTwo.getId(), td.snapTime).get();
    assertThat(snapshotSetAggregationTwo.getInstruments().keySet()).containsOnly(td.appleInstrumentOne, td.starbucksInstrumentThreeId.getId());
    assertThat(snapshotSetAggregationTwo.getInstruments().entrySet()).extracting(x -> x.getValue()).containsOnly(td.thirdTick10, td.instrumentThreeTickTwo400);
  }

  @Test
  public void testGetPrecomputedSnapSets() {
    tdd.insertTestDataSetOne();
    tdd.insertTestDataSetTwo();
    tdd.insertTestDataSetThree();

    // exec
    List<KeyValue<InstrumentId, Optional<ValueAndTimestamp<InstrumentTickBD>>>> snapSetAt = instrumentService.findAndCalculateSnapAt(td.snapSetConfigTwo.getId(), td.snapTime);

    // test
    assertThat(snapSetAt).extracting(x -> x.key).containsExactlyInAnyOrder(td.appleInstrumentOneId, td.starbucksInstrumentThreeId);
    assertThat(snapSetAt).extracting(x -> x.value.get().value().getPrice()).containsExactlyInAnyOrder(td.thirdTick10, td.instrumentThreeTickTwo400);
  }


  @Test
  void aggregateEarlyDataTest() {
    tdd.insertEarlierTestDataInOrder();

    // exec
    Optional<SnapshotSetAggregation> snapSetAt = snapSetsPrecomputedSimpleService.simpleFetchSnapAt(td.snapSetConfigTwo.getId(), td.snapTime);

    // test
    assertThat(snapSetAt).isPresent();
    assertThat(snapSetAt.get().getInstruments())
            .as("There should be multiple instruments in this snap by this stream time").hasSize(2);
    assertThat(snapSetAt.get().getInstruments().keySet()).containsExactlyInAnyOrder(td.appleInstrumentOne, td.starbucksInstrumentThreeId.getId());
    assertThat(snapSetAt.get().getInstruments().values()).containsExactlyInAnyOrder(td.secondTick50, td.instrumentThreeTickOne490);
  }

}
