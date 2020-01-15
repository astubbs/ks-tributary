package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TestRecord;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.OptionalAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.time.Duration.ofHours;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class PublicInterfaceTests extends TestBase {

  @Inject
  protected LatestInstrumentWindowsService instrumentService;

  @Inject
  protected SnapshotTopologySettings settings;

  @Inject
  protected TestData td;

  @Inject
  protected TestDataDriver tdd;

  @Inject
  TopologyTestDriver testDriver;

  /**
   * assert our final snap outputs are correct
   */
  @Test
  public void testLastPricesForSnapsPublishedToSnapsTopicSimple() {
    tdd.insertTestDataSetOne();

    // Because we are testing the snap output topic, we must advance time to trigger the window closes handlers to fire,
    // which are triggered only upon data arrival in the same stream tasks
    int hours = 3;
    tdd.advanceStreamTimeTo(tdd.baseTimePlusHours(hours).plus(settings.getWindowGrace()));

    List<TestRecord<Windowed<InstrumentId>, InstrumentTickBD>> output2 = tdd.outputTopic.readRecordsToList();
    log.info("output topic results: " + output2.toString());
    assertThat(output2).hasSize((int) ofHours(hours).dividedBy(settings.getAdvanceBy()));
    assertThat(output2).extracting("value").doesNotContain(td.dummyTickValue);
  }

  /**
   * Solve the back fill problem via using larger windows, instead of querying latest value ktable when ticker is missing
   */
  @Test
  @Disabled("TODO, if when running from fresh start, results should be correct")
  public void testBackFillViaMultipleDays() {
  }

  /**
   * KS Windows are inclusive to exclusive - i.e. 3pm to 4pm - tickets stamped 3pm are included, but 4pm are excluded
   */
  @Test
  @DisplayName("Requested snap time of 4pm, excludes tickets stamped at 4pm exactly (as technically 4pm is now outside the snap time bucket")
  public void requestedSnapTimeExclusive() {
    tdd.tickTopicPipe(td.appleInstrumentOne, "1", td.baseTime);
    tdd.tickTopicPipe(td.googleInstrumentTwo, "3", td.baseTime.plus(ofHours(4)));
    tdd.tickTopicPipe(td.appleInstrumentOne, "2", td.snapTime);

    // all
    ArrayList<KeyValue<InstrumentId, InstrumentTickBD>> allKeysForSnapAt = instrumentService.findTicksForAllInstrumentWindowsEndingAt(td.snapTime);

    // test
    ListAssert<KeyValue<InstrumentId, InstrumentTickBD>> keyValueListAssert = assertThat(allKeysForSnapAt).as("Has both keys, but excludes the last update for first key, as it's outside the window").hasSize(2);
    keyValueListAssert.extracting(x -> x.value.getId()).containsOnly(td.appleInstrumentOneId, td.googleInstrumentThreeId);
    keyValueListAssert.extracting(x -> x.value.getPrice()).containsOnly("1", "3");

    // single
    Optional<ValueAndTimestamp<InstrumentTickBD>> snapForSingleKeyAt = instrumentService.findTickForThisInstrumentForWindowEndingAt(td.appleInstrumentOneId, td.snapTime);

    // test style 1
    assertThat(snapForSingleKeyAt).hasValueSatisfying(x -> {
      Assertions.assertThat(x.value().getId()).isEqualTo(td.appleInstrumentOneId);
      Assertions.assertThat(x.value().getPrice()).isEqualTo("1");
    });

    // test style 2
    assertThat(snapForSingleKeyAt).get().extracting(x -> x.value().getId(), x -> x.value().getPrice()).containsOnly(td.appleInstrumentOneId, "1");

    // test style 3
    OptionalAssert<ValueAndTimestamp<InstrumentTickBD>> keyValueObjectAssert = assertThat(snapForSingleKeyAt).isPresent();
    keyValueObjectAssert.hasValueSatisfying(x -> Assertions.assertThat(x.value().getId()).isEqualTo(td.appleInstrumentOneId));
    keyValueObjectAssert.hasValueSatisfying(x -> Assertions.assertThat(x.value().getPrice()).isEqualTo("1"));
  }

}
