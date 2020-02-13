package io.confluent.ps.streams.referenceapp.finance;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.time.Duration.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class LatestValueAndOutputStreamTests extends DaggerTestBase {

  @Inject
  LatestInstrumentWindowsService instrumentService;

  @Inject
  SnapshotStoreProvider stores;

  @Inject
  TestData td;

  @Inject
  SnapshotTopologySettings settings;

  @Test
  @DisplayName("Number of buckets per X")
  public void testNumberOfBucketsInDay() {
    tdd.tickTopicPipe("AAPL", "1", td.baseTime);

    Duration advanceByWindowSize = settings.getWindowSize();
    Duration windowAdvancesBy = settings.getAdvanceBy();
    int expectedBuckets = ((int) advanceByWindowSize.dividedBy(windowAdvancesBy));

    // one window size plus grace
    tdd.advanceStreamTimeBy(advanceByWindowSize);

    //
    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> outputFirst = tdd.readOutputNonSynthetic();
    Instant oldestMatchingWindowStartTime = td.baseTimeMinusWindowSize.plus(ofMinutes(30)); // tick of base time is excluded in "window size" time ago, because windows end exclusively (are not inclusive)
    assertThat(outputFirst).hasSize(expectedBuckets - 1) // -1 because the last windows grace period hasn't been covered
            .first().extracting((x) -> x.key.window().startTime()).isEqualTo(oldestMatchingWindowStartTime);

    // advance including grace period
    tdd.advanceStreamTimeBy(advanceByWindowSize.plus(settings.getWindowGrace()));

    //
    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> outputSecond = tdd.readOutputNonSynthetic();
    assertThat(outputSecond).hasSize(1) // only moved stream forward enough to close on extra window
            .first().extracting((x) -> x.key.window().startTime()).isEqualTo(td.baseTime); // results now most recent window because we're past the grace period

    // combined
    ArrayList<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> combined = Lists.newArrayList(outputFirst);
    combined.addAll(outputSecond);
    assertThat(combined).hasSize(expectedBuckets); // now have all 48 half our windows in a 24 hour period
  }

  @Test()
  @DisplayName("Windows don't close even with synthetic events, until the grace period is also over")
  public void testWindowCloseAfterGrace() {
    tdd.tickTopicPipe("AAPL", "1", td.baseTime.minus(ofMillis(1))); // ever so slightly in window ending at base time

    // read output - nothing's closed yet
    assertThat(tdd.outputTopic.readKeyValuesToList()).hasSize(0);

    // advance over window close (past the hour mark (base time time (7am + 1 minute)
    Duration streamTime = ofMinutes(1);
    tdd.advanceStreamTimeBy(streamTime);

    // nothing yet
    assertThat(tdd.outputTopic.readKeyValuesToList()).hasSize(0);

    // advance to grace period
    Duration plus = settings.getWindowGrace();
    tdd.advanceStreamTimeBy(plus);

    // window should be emitted now, and the window should be exactly basetime minus windowSize
    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> actual = tdd.readOutputNonSynthetic();
    int expectedBucketsOnOutputTopicAfterJustGracePeriod = 1;
    assertThat(actual).hasSize(expectedBucketsOnOutputTopicAfterJustGracePeriod).first()
            .extracting((x) -> x.key.window().startTime()).isEqualTo(td.baseTimeMinusWindowSize);

    // move stream time forward
    Duration advanceByHours = plus.plus(ofHours(1));
    tdd.advanceStreamTimeBy(advanceByHours);

    // calculate how many buckets we expect to get back
    Duration windowAdvancesBy = settings.getAdvanceBy();
    int expectedSuppressedClosedBuckets = ((int) advanceByHours.minus(settings.getWindowGrace()).dividedBy(windowAdvancesBy));

    // read
    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> output = tdd.readOutputNonSynthetic();
    assertThat(output).hasSize(expectedSuppressedClosedBuckets).first()
            .extracting((x) -> x.key.window().startTime()).isEqualTo(td.baseTimeMinusWindowSize.plus(windowAdvancesBy));

    // combine our read outputs, because we already read some values off of the topic
    ArrayList<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> combinedOutputTopic = Lists.newArrayList(output);
    combinedOutputTopic.addAll(actual);

    //
    int finalNumberOfOutputBuckets = expectedSuppressedClosedBuckets + expectedBucketsOnOutputTopicAfterJustGracePeriod;
    assertThat(combinedOutputTopic).hasSize(finalNumberOfOutputBuckets);
  }

  /**
   * Will only match one window, due to the timing of windows published
   */
  @Test
  @DisplayName("Returned window should catch only one tick, due to timing of ticks")
  public void testMatchOneTick() {
    //
    tdd.insertTestDataSetOne();

    Instant windowEndTime = td.baseTime.plus(ofMinutes(30));
    Optional<ValueAndTimestamp<InstrumentTickBD>> instrumentForWindowEndingAt = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.appleInstrumentOneId, windowEndTime);

    assertThat(instrumentForWindowEndingAt).as("Only a single window matched which contains only our initial tick")
            .get().extracting(x -> x.value().getPrice()).isEqualTo(td.firstTick20);
  }

  /**
   * will get just the matching window (the window which ends at the specified snap time)
   */
  @Test
  public void testSingleMatchingWindowFromKey() {
    tdd.insertTestDataSetOne();

    Instant windowEndingIn30Minutes = td.baseTime.plus(30, ChronoUnit.MINUTES); // aka requested snap time

    Optional<ValueAndTimestamp<InstrumentTickBD>> valueKeyForWindowEndingAt = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.appleInstrumentOneId, windowEndingIn30Minutes);
    ValueAndTimestamp<InstrumentTickBD> firstWindowValue = valueKeyForWindowEndingAt.get();
    Assertions.assertThat(firstWindowValue.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(firstWindowValue.timestamp()).isEqualTo(td.firstTickTimeStamp.toEpochMilli());
  }

  @Test
  @DisplayName("Single matching windows for specific keys when multiple keys in data published OUT OF order")
  public void dataArrivingInGraceAndOutOfGracePeriod() {
    tdd.tickTopicPipe(td.appleInstrumentOne, td.firstTick20, td.firstTickTimeStamp);
    tdd.tickTopicPipe(td.appleInstrumentOne, td.secondTick50, td.firstTickTimeStamp.plus(settings.getAdvanceBy().plusMinutes(1)));

    Instant windowEndingIn30Minutes = td.baseTime.plus(ofMinutes(30)); // aka requested snap time

    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> keyValues = tdd.readOutputNonSynthetic();

    List<KeyValue<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>>> allWindowsOne = ImmutableList.copyOf(stores.latestInstrumentWindowsStore().fetchAll(0, Long.MAX_VALUE));
    assertThat(allWindowsOne).first().extracting(x -> x.value.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(allWindowsOne.get(1)).as("second window is replaced by second tick")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.secondTick50);
    assertThat(allWindowsOne.get(2)).as("third window is replaced by second tick")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.secondTick50);

    // test getting exact snap
    ValueAndTimestamp<InstrumentTickBD> firstWindowValue = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.appleInstrumentOneId, windowEndingIn30Minutes).get();
    Assertions.assertThat(firstWindowValue.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(firstWindowValue.timestamp()).isEqualTo(td.firstTickTimeStamp.toEpochMilli());

    // move time forward within grace
    tdd.advanceStreamTimeTo(td.firstTickTimeStamp.plus(settings.getAdvanceBy().multipliedBy(2).plus(settings.getWindowGrace().minusMinutes(2))));

    // send late message within grace period message
    Instant plusOne = td.firstTickTimeStamp.plus(settings.getAdvanceBy().multipliedBy(2).minusMinutes(1)); // will fit in 2nd window, but is late within grace
    tdd.tickTopicPipe(td.appleInstrumentOne, td.thirdTick10, plusOne);

    // test
    List<KeyValue<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>>> allWindowsTwo = ImmutableList.copyOf(stores.latestInstrumentWindowsStore().fetchAll(0, Long.MAX_VALUE));
    assertThat(allWindowsTwo).first().extracting(x -> x.value.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(allWindowsTwo.get(1)).as("second window is replaced by third tick")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.thirdTick10);
    assertThat(allWindowsTwo.get(2)).as("third window is replaced by third tick")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.thirdTick10);


    // advance past grace, won't land in window
    tdd.advanceStreamTimeTo(td.firstTickTimeStamp.plus(settings.getAdvanceBy().multipliedBy(2).plus(settings.getWindowGrace().plusMinutes(1)))); // move time forward within grace

    // send late message outside of grace
    Instant plusTwo = td.firstTickTimeStamp.plus(settings.getAdvanceBy().multipliedBy(2).minusMinutes(1)); // will fit in 2nd window, but is late within grace
    tdd.tickTopicPipe(td.appleInstrumentOne, td.fourthTick12, plusTwo);

    // test
    List<KeyValue<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>>> allWindowsThree = ImmutableList.copyOf(stores.latestInstrumentWindowsStore().fetchAll(0, Long.MAX_VALUE));
    assertThat(allWindowsThree).first().extracting(x -> x.value.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(allWindowsThree.get(1)).as("second window has not changed")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.thirdTick10);
    assertThat(allWindowsThree.get(3)).as("third window is replaced")
            .extracting(x -> x.value.value().getPrice()).isEqualTo(td.fourthTick12);
  }

  /**
   * will get just the matching window (the window which ends at the specified snap time)
   */
  @Test
  @DisplayName("Single matching windows for specific keys when multiple keys in data published in order")
  public void multipleKeysInOrder() {
    tdd.insertAllTestDataInOrder();

    Instant windowEndingIn30Minutes = td.baseTime.plus(ofMinutes(30)); // aka requested snap time

    // make sure all windows are closed that we're looking for
    List<KeyValue<Windowed<InstrumentId>, InstrumentTickBD>> keyValues = tdd.readOutputNonSynthetic();

    List<KeyValue<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>>> allWindows = ImmutableList.copyOf(stores.latestInstrumentWindowsStore().fetchAll(0, Long.MAX_VALUE));
    List<KeyValue<Windowed<InstrumentId>, ValueAndTimestamp<InstrumentTickBD>>> collect = allWindows.stream().filter(x -> x.key.key().equals(td.googleInstrumentThreeId)).collect(Collectors.toList());


    // instrument one
    ValueAndTimestamp<InstrumentTickBD> firstWindowValue = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.appleInstrumentOneId, windowEndingIn30Minutes).get();
    Assertions.assertThat(firstWindowValue.value().getPrice()).isEqualTo(td.firstTick20);
    assertThat(firstWindowValue.timestamp()).isEqualTo(td.firstTickTimeStamp.toEpochMilli());

    // instrument two
    ValueAndTimestamp<InstrumentTickBD> instrumentTwoResults = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.googleInstrumentThreeId, windowEndingIn30Minutes).get();
    Assertions.assertThat(instrumentTwoResults.value().getPrice()).isEqualTo("120");
    assertThat(instrumentTwoResults.timestamp()).isEqualTo(td.firstTickTimeStamp.toEpochMilli());

    // TODO use an assertJ allOf condition to combine these two
    Assertions.assertThat(instrumentTwoResults.value().getPrice()).as("second instrument is correct")
            .isEqualTo("120");
    assertThat(instrumentTwoResults.timestamp()).as("second instrument is correct")
            .isEqualTo(td.firstTickTimeStamp.toEpochMilli());
  }

  @Test
  @DisplayName("Capture windows only which contain our final two ticks")
  public void testMatchOnlyLastTwoWindows() {
    tdd.insertTestDataSetOne();

    // windows which end between 10 and 1030
    // TODO derive magic numbers from Topology settings instead
    Instant twentyOneHoursAgo = td.baseTime.minus(ofHours(21));
    Instant twentyPointFiveHoursAgo = td.baseTime.minus(ofMinutes((int) (20.5 * 60)));
    WindowStoreIterator<ValueAndTimestamp<InstrumentTickBD>> lastWindowFetch = stores.latestInstrumentWindowsStore().fetch(td.appleInstrumentOneId, twentyOneHoursAgo.toEpochMilli(),
            twentyPointFiveHoursAgo.toEpochMilli());

    List<KeyValue<Long, ValueAndTimestamp<InstrumentTickBD>>> lastWindowList = ImmutableList.copyOf(lastWindowFetch);
    assertThat(lastWindowList).hasSize(2);
  }


}
