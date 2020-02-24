package io.confluent.ps.streams.referenceapp.school;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.denormalisation.model.*;
import io.confluent.ps.streams.referenceapp.denormilsation.topology.DenormalisationTopology;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
//import org.apache.kafka.streams.TestInputTopic;
//import org.apache.kafka.streams.TestOutputTopic;
//import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;

import static io.confluent.ps.streams.referenceapp.denormilsation.topology.DenormalisationTopology.suppressionWindowTime;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
public class DenormalisationTopologyTest extends GuiceInjectedTestBase {

  @Inject
  TestDataDriver tdd;

  @Inject
  TopologyTestDriver td;

  @Inject
  KSUtils ksutils;

  TestInputTopic<DocumentId, ComponentOne> comp_a_topic;
  TestInputTopic<DocumentId, ComponentTwo> comp_b_topic;
  TestInputTopic<DocumentId, ComponentThree> comp_c_topic;

  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicUntil;
  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicClosses;
  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicCustom;

  @BeforeEach
  void setup() {
//    var keySerde = ksutils.<GoalId, GoalId>wrappingSerdeFor(true);
//    var valueSerde = ksutils.<GoalEvent, GoalEventWrapped>wrappingSerdeFor(false);
    var keySerde = ksutils.<DocumentId>serdeFor(true);
    var compOneSerde = ksutils.<ComponentOne>serdeFor(false);
    var compTwoSerde = ksutils.<ComponentTwo>serdeFor(false);
    var compThreeSerde = ksutils.<ComponentThree>serdeFor(false);
    var aggSerde = ksutils.<ComponentAggregate>serdeFor(false);

    comp_a_topic = td.createInputTopic(DenormalisationTopology.COMP_A_TOPIC, keySerde.serializer(), compOneSerde.serializer());
    comp_b_topic = td.createInputTopic(DenormalisationTopology.COMP_B_TOPIC, keySerde.serializer(), compTwoSerde.serializer());
    comp_c_topic = td.createInputTopic(DenormalisationTopology.COMP_C_TOPIC, keySerde.serializer(), compThreeSerde.serializer());

    aggTopicUntil = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_SUPPRESS_UNTIL, keySerde.deserializer(), aggSerde.deserializer());
    aggTopicClosses = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_SUPPRESS_CLOSES, keySerde.deserializer(), aggSerde.deserializer());
    aggTopicCustom = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_CUSTOM, keySerde.deserializer(), aggSerde.deserializer());
  }

  @Test
  void dataAggregatorSimpleSuppressUntilStreamTime() {
    test(aggTopicUntil, false);
  }

  private void test(TestOutputTopic<DocumentId, ComponentAggregate> outputTopic, boolean useWallClock) {
    // send through some data
    val parent = DocumentId.newBuilder().setId("a-school").build();
    val compOne = ComponentOne.newBuilder().setCode("code").setParentId(parent).setName("sdsd").build();
    val compTwo = ComponentTwo.newBuilder().setParentId(parent).setCode("code").setName("name").build();

    long now = System.currentTimeMillis();

    comp_a_topic.pipeInput(parent, compOne, now);
    comp_b_topic.pipeInput(parent, compTwo, now);

    // check records are suppressed
    assertThat(outputTopic.readValuesToList()).hasSize(0);

    // test nothing is emitted, beyond the suppression window, unless we send messages
    // (this slows the test down a "lot")
    //    Awaitility.setDefaultTimeout(ofSeconds(15));
    Duration waitUntil = suppressionWindowTime.plus(ofMillis(500));
    log.debug("Start testing lack of messages (suppression window is {} and we will wait up to {})...", suppressionWindowTime, waitUntil);
    await().during(waitUntil)
//            .pollInterval(ofSeconds(1))
            .conditionEvaluationListener(condition -> log.debug("Still no message emitted... (elapsed:{}ms, remaining:{}ms)", condition.getElapsedTimeInMS(), condition.getRemainingTimeInMS()))
            .until(() -> outputTopic.readValuesToList().size() == 0);

    // move time forward
    Duration twoMinutes = ofMinutes(2);
    if (useWallClock) {
      td.advanceWallClockTime(twoMinutes);
    } else {
      compOne.setName("a-different-name");
      long moveTo = now + twoMinutes.toMillis();
      comp_a_topic.pipeInput(parent, compOne, moveTo);
    }
//    timeMover.apply(KeyValue.pair(parent, compOne), now);


    // check record emitted
    List<ComponentAggregate> records = outputTopic.readValuesToList();
    assertThat(records).hasSize(1);
    ComponentAggregate actual = records.stream().findFirst().get();
    assertThat(actual.getOne()).isEqualTo(compOne);
    assertThat(actual.getTwo()).isEqualTo(compTwo);
    assertThat(actual.getThree()).isNull();
  }

  @Test
  void suppressionWindowLookupStreamTime() {
    test(aggTopicClosses, false);
  }

  @Test
  void customWallClockTimer() {
    test(aggTopicCustom, true);
  }

}
