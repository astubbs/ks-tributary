package io.confluent.ps.streams.referenceapp.finance;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.github.jukkakarvanen.kafka.streams.test.TestRecord;
import com.google.common.collect.ImmutableSet;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceIndividual;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSharded;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSimple;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannels;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.*;
import io.confluent.ps.streams.referenceapp.finance.topologies.*;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyPrecomputerSimple;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyParent;
import io.confluent.ps.streams.referenceapp.utils.WallClockProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class ChannelTests extends TestBase {

  @Inject
  protected TestData td;

  @Inject
  MockChannelServiceSharded csShardedClients;

  @Inject
  protected MockChannelServiceSimple csSimpleClients;

  @Inject
  protected MockChannelServiceIndividual csIndividualClients;

  @Inject
  SnapshotSetsPrecomputedSimpleService snapSetsPrecomputedSimpleService;

  @Inject
  SnapshotSetsPrecomputedShardedService snapSetsPrecomputedShardedService;

  @Inject
  LatestInstrumentWindowsService latestInstrumentWindowsService;

  @Inject
  SnapshotTopologyLatestWindows snapshotTopologyLatestWindows;

  @Inject
  SnapshotTopologyPrecomputerSimple snapTopologyPrecomputerSimple;

  @Inject
  SnapshotTopologyPrecomputerSharded snapTopologyPrecomputerSharded;

  @Test
  void testPrecomputedSnapSimpleTwo() {
    testPrecomputedSnapSimpleAndSharded(csSimpleClients, td.channelsListSimple);
  }

  @Test
  void testPrecomputedSnapShardedTwo() {
    testPrecomputedSnapSimpleAndSharded(csShardedClients, td.channelsListSharded);
  }

  void testPrecomputedSnapSimpleAndSharded(ChannelService cs, List<MockChannels> channelsLists) {
    tdd.insertAllTestDataInOrder();
    MockChannels channelOne = channelsLists.get(0);
    MockChannels channelTwo = channelsLists.get(1);

    List<MockChannels> channelSubscriptions = cs.findChannelSubscriptions(td.snapSetConfigOne.getId());
    assertThat(channelSubscriptions).hasSizeGreaterThan(0);
    MockChannels actual = channelSubscriptions.stream().findFirst().get();
    assertThat(actual.getChannelId()).as("Singleton setup sanity check").isSameAs(channelOne.getChannelId());
    assertThat(actual).as("Singleton setup sanity check").isSameAs(channelOne);
    assertThat(channelSubscriptions).extracting(MockChannels::getChannelId)
            .containsExactlyInAnyOrder(channelOne.getChannelId(), channelTwo.getChannelId());

    //
    List<Object> channelOutputBufferOne = channelOne.getChannelOutputBuffer();
    List<Object> channelOutputBufferTwo = channelTwo.getChannelOutputBuffer();

    //
    assertThat(channelOutputBufferOne).hasSize(3);
    assertThat(channelOutputBufferOne).extracting("id").containsOnly(td.appleInstrumentOneId);

    //
    assertThat(channelOutputBufferTwo).hasSize(8);
    assertThat(channelOutputBufferTwo).extracting("id.id").containsOnly(td.appleInstrumentOne, td.starbucksInstrumentThreeId.getId());
    assertThat(channelOutputBufferTwo).filteredOn("id.id", td.appleInstrumentOne).last().extracting("price").isEqualTo("10");
    assertThat(channelOutputBufferTwo).filteredOn("id.id", td.starbucksInstrumentThreeId.getId()).last().extracting("price").isEqualTo("400");
  }

  @Test
  void testPrecomputedSnapIndividual() {
    tdd.insertAllTestDataInOrder();

    // check channel contains bootstrap info from earlier data, and new realtime tdata
    List<MockChannels> channelSubscriptions = csIndividualClients.findExplicitChannelSubscriptionsForKey(td.googleInstrumentThreeId);
    assertThat(channelSubscriptions)
            .hasSize(2)
            .extracting(MockChannels::getChannelId)
            .containsExactly(td.channelThreeIndividualKey.getChannelId(), td.channelFiveDataArrivesAfterWindowClose.getChannelId());

    List<Object> channelOutputBufferThree = td.channelThreeIndividualKey.getChannelOutputBuffer();

    // specific key subscriptions
    assertThat(channelOutputBufferThree).isNotEmpty();
    assertThat(channelOutputBufferThree).extracting("key").containsOnly(td.googleInstrumentThreeId);
    assertThat(channelOutputBufferThree).extracting("value.price").containsExactly("120", "150", "130");
  }

  @Test
  @DisplayName("Individual: Check no live records are sent after subscribed window is closed")
  void checkWindowClosingIndividual() {
    checkWindowClosingExecute(latestInstrumentWindowsService);
  }

  @Test
  @DisplayName("Simple: Check no live records are sent after subscribed window is closed")
  void checkWindowClosingSimple() {
    checkWindowClosingExecute(snapSetsPrecomputedSimpleService);
  }

  @Test
  @DisplayName("Sharded: Check no live records are sent after subscribed window is closed")
  void checkWindowClosingSharded() {
    checkWindowClosingExecute(snapSetsPrecomputedShardedService);
  }

  private int offset = 0; // inside window

  int getOffset() {
    return offset;
  }

  void checkWindowClosingExecute(SnapshotSetAggregateSupplier aggregateSupplier) {

    // outside the window
    offset = 1;

    WallClockProvider wallClockProviderTwo = () -> td.snapTime.plusMillis(getOffset());

    snapshotTopologyLatestWindows.setWallClockProvider(wallClockProviderTwo);
    snapTopologyPrecomputerSharded.setWallClockProvider(wallClockProviderTwo);
    snapTopologyPrecomputerSimple.setWallClockProvider(wallClockProviderTwo);

    tdd.tickTopicPipe(td.starbucksInstrumentThree, "400", tdd.baseTimePlusHours(1)); // very late arriving message

    // check the aggregate which is stored matches
    assertThat(td.channelFiveDataArrivesAfterWindowClose.getChannelOutputBuffer()).as("Channel is empty because subscription time is over / message is suppressed").isEmpty();

    // TODO should late arriving data be aggregated, but not send to channel?
    assertThat(aggregateSupplier.findSnapSetAtWindowEndTime(td.snapSetConfigTwo.getId(), td.snapTime).get().getInstruments())
            .as("Sill aggregate the late data")
            .isNotEmpty();


    // inside window
    offset = -1;
    tdd.tickTopicPipe(td.starbucksInstrumentThree, "400", tdd.baseTimePlusHours(1)); // very late arriving message

    // check the aggregate which is stored matches
    assertThat(td.channelFiveDataArrivesAfterWindowClose.getChannelOutputBuffer()).as("Channel is not empty because subscription time is over / message is suppressed").isNotEmpty();

  }

  @Test
  void channelBootstrapSharded() {
    channelBootstrapTriggeredByArrivingData(csShardedClients, snapTopologyPrecomputerSharded, snapSetsPrecomputedShardedService);
  }

  @Test
  void channelBootstrapSimple() {
    channelBootstrapTriggeredByArrivingData(csSimpleClients, snapTopologyPrecomputerSimple, snapSetsPrecomputedSimpleService);
  }

  /**
   * Check bootstrap data is sent correctly. Like a lot of things in streams, this is triggered by data arriving on the stream. If a new channel is added, but no data arrives, that channel never receives anything.
   * <p>
   * It could be made such that a new channel queries for it's data when it joins, but as this will be in a different thread, it won't be synchornised with arriving data. So, a merge step would have to be performed somewhere with the synchronised bootstrap. Otherwise, leave the bootstrap out of the real time stream, and just merge anyway. This would simplify the topology code, at the cost of merge complexity elsewhere, either in the API layer, or on the client.
   * <p>
   * An alternative approach, is to publish synthetic events to the tick topic to trigger the bootstrap, upon channel subscription. This has the advantage of no merging required, and realtime boostrap publishing, regardless of arrival of natural data. At the cost of polluting the tick topic with synthetic events. This can be mitigated by having a copy of the original topic, where synthetic events are also published, in order to keep the natural input pure. The main deal breaker for synthetic event strategy, is that you must be careful to send synthetic events to EVERY partition in the topic. The only way to do this reliable to is to selectively produce to every partition, not to the topic.
   * <p>
   * In terms of Avro, the synthetic approach could be achieved with an message envelop, into which either the synthetic trigger goes, or the natural event.
   * <p>
   * Another alternative, is to setup a {@link org.apache.kafka.streams.processor.Punctuator} to poll the channel stream, to see if any new channels have arrived, and if so publish the last bootstrap. This is the simplest of the approaches, but at the cost of cpu polling empty synchronised collections, or collections where the boolean indicates the channel isn't new. The nice thing about this, is that when combined with the above approach, the channel either recieves the boostrap as soon as a triggering event arrives, or when the punctuator runs, e.g. at most 1 second.
   *
   * @see PrecomputedSnapshotSetTests#aggregateEarlyDataTest
   */
  void channelBootstrapTriggeredByArrivingData(ChannelService csSimpleClients, SnapshotTopologyParent snapTopologyLatestWindows, SnapshotSetAggregateSupplier aggregateSupplier) {
    // insert test data, without channel four subscribed
    tdd.insertEarlierTestDataInOrder();

    // subscribe channel four
    MockChannels theChannel = td.channelFourLateSub;
    SnapshotSetId snapId = td.snapSetConfigTwo.getId();
    // subscribe channel four to the snap set
    when(csSimpleClients.findChannelSubscriptions(snapId)).thenReturn(ImmutableList.of(theChannel));
    // also subscribe channel four to the two keys within, for the individual snap topology test
    when(csSimpleClients.findExplicitChannelSubscriptionsForKey(td.appleInstrumentOneId)).thenReturn(ImmutableList.of(theChannel));
    when(csSimpleClients.findExplicitChannelSubscriptionsForKey(td.starbucksInstrumentThreeId)).thenReturn(ImmutableList.of(theChannel));

    // test channel empty as bootstrap not yet triggered by trigger data
    List<Object> channel = theChannel.getChannelOutputBuffer();
    ImmutableSet<Object> output = ImmutableSet.copyOf(channel);
    assertThat(output).isEmpty();

    // override wall time to control conditional realtime updates, make sure we are still before window end time
    snapTopologyLatestWindows.setWallClockProvider(new WallClockProvider() {
      @Override
      public Instant getNow() {
        return td.snapTime.minusMillis(1);
      }
    });

    // insert single message to trigger bootstrap. Send two, in order to trigger both bootstraps for the individual topology (no snap set subscriptions, only individual}
    TestRecord<InstrumentId, InstrumentTickBD> tick = tdd.tickFor(td.starbucksInstrumentThree, "490", tdd.baseTimePlusHours(1));
    tdd.tickTopicPipe(tick); // same as last tick, minimal change

    // record channel output
    ImmutableSet<Object> firstActualOutput = ImmutableSet.copyOf(channel);

    // check the aggregate which is stored matches
    SnapshotSetAggregation snapshotSetAggregation = aggregateSupplier.findSnapSetAtWindowEndTime(td.snapSetConfigTwo.getId(), td.snapTime).get();
    assertThat(snapshotSetAggregation.getInstruments()).contains(
            entry(td.appleInstrumentOne, td.secondTick50),
            entry(td.starbucksInstrumentThree, "490")
    );

    // check the tick matches
    InstrumentTick actual = (InstrumentTick) channel.get(1);
    assertThat(actual).isEqualToComparingFieldByField(tick.getValue()); // checks due to complexity of object hierarchy
    assertThat(actual).isEqualTo(tick.getValue());

    // test channel now has bootstrap first, and then the realtime data from the last insert
    boolean invidividualSubsciptionTopology = aggregateSupplier.getClass().isAssignableFrom(LatestInstrumentWindowsService.class);
    if (invidividualSubsciptionTopology) {
      // cheat a little, this topology has mildly different behaviour
      assertThat(channel).extracting("value")
              .hasSize(1)
              .containsExactly(tick.getValue());
    } else {
      assertThat(channel)
              .hasSize(2)
              .containsExactly(snapshotSetAggregation, tick.getValue());
    }

    // add more data
    tdd.insertLaterTestDataInOrder();

    // test channel has realtime data just added
    ImmutableSet<Object> latestOutput = ImmutableSet.copyOf(channel);

    if (invidividualSubsciptionTopology) {
      assertThat(latestOutput).as("test channel now has previous plus realtime data just added").hasSize(3).extracting("value").contains(
              tdd.laterDataStream.get(0).getValue(),
              tdd.laterDataStream.get(2).getValue()
      );
    } else {
      Object[] firstOutputRemoved = latestOutput.stream().filter(x -> !firstActualOutput.contains(x)).toArray();
      assertThat(firstOutputRemoved).as("test channel has realtime data just added").hasSize(2).containsExactly(
              tdd.laterDataStream.get(0).getValue(),
              tdd.laterDataStream.get(2).getValue()
      );
    }

  }

  /**
   * TODO punctuator implementation of publishing bootstraps to new data
   */
  @Test
  @Disabled
  void sendBootstrapToChannelUponTimeoutWithoutEventTrigger() {
    assertThat(true).isFalse();
  }

  /**
   * TODO check that the channel entries are correct in respect do duplicates
   */
  @Test
  @Disabled
  void channelCheckDuplicateEntries() {
  }

  /**
   * TODO check that the channel receives the latest available bootstrap data, if it's not subscribed to a specific snap time
   */
  @Test
  @Disabled
  void check() {
  }

}
