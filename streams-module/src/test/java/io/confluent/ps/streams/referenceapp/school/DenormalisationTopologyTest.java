package io.confluent.ps.streams.referenceapp.school;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.denormalisation.model.*;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.denormilsation.topology.DenormalisationTopology;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;

import static io.confluent.ps.streams.referenceapp.denormilsation.topology.DenormalisationTopology.suppressionWindowTime;
import static java.time.Duration.*;
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

  TestInputTopic<DocumentId, ComponentOne> orgTopic;
  TestInputTopic<DocumentId, ComponentTwo> statusTopic;
  TestInputTopic<DocumentId, ComponentThree> subtypeTopic;

  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicUntil;
  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicClosses;
  TestOutputTopic<DocumentId, ComponentAggregate> aggTopicCustom;

  @BeforeEach
  void setup() {
//    var keySerde = ksutils.<GoalId, GoalId>wrappingSerdeFor(true);
//    var valueSerde = ksutils.<GoalEvent, GoalEventWrapped>wrappingSerdeFor(false);
    var keySerde = ksutils.<DocumentId>serdeFor(true);
    var orgSerde = ksutils.<ComponentOne>serdeFor(false);
    var statusSerde = ksutils.<ComponentTwo>serdeFor(false);
    var subtypeSerde = ksutils.<ComponentThree>serdeFor(false);
    var aggSerde = ksutils.<ComponentAggregate>serdeFor(false);

    orgTopic = td.createInputTopic(DenormalisationTopology.ORG_UNIT_TOPIC, keySerde.serializer(), orgSerde.serializer());
    statusTopic = td.createInputTopic(DenormalisationTopology.STATUS_CODE_TOPIC, keySerde.serializer(), statusSerde.serializer());
    subtypeTopic = td.createInputTopic(DenormalisationTopology.SUP_TYPE_TOPIC, keySerde.serializer(), subtypeSerde.serializer());

    aggTopicUntil = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_SUPPRESS_UNTIL, keySerde.deserializer(), aggSerde.deserializer());
    aggTopicClosses = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_SUPPRESS_CLOSES, keySerde.deserializer(), aggSerde.deserializer());
    aggTopicCustom = td.createOutputTopic(DenormalisationTopology.AGGREGATE_UPDATES_TOPIC_CUSTOM, keySerde.deserializer(), aggSerde.deserializer());
  }

  @Test
  void dataAggregatorSimpleSuppressUntilStreamTime() {
    test(aggTopicUntil);
  }

  private void test(TestOutputTopic<DocumentId, ComponentAggregate> output) {
    // send through some data
    val parent = DocumentId.newBuilder().setId("a-school").build();
    val anOrgUnit = ComponentOne.newBuilder().setCode("code").setParentId(parent).setName("sdsd").build();
    ComponentThree subType = ComponentThree.newBuilder().setParentId(parent).setCode("code").setName("name").build();

    orgTopic.pipeInput(parent, anOrgUnit);
    subtypeTopic.pipeInput(parent, subType);

    // check records are suppressed
    assertThat(output.readValuesToList()).hasSize(0);

    // test nothing is emitted, beyond the suppression window, unless we send messages
    // (this slows the test down a "lot")
    //    Awaitility.setDefaultTimeout(ofSeconds(15));
    Duration waitUntil = suppressionWindowTime.plus(ofMillis(500));
    log.debug("Start testing lack of messages (suppression window is {} and we will wait up to {})...", suppressionWindowTime, waitUntil);
    await().during(waitUntil)
//            .pollInterval(ofSeconds(1))
            .conditionEvaluationListener(condition -> log.debug("Still no message emitted... (elapsed:{}ms, remaining:{}ms)", condition.getElapsedTimeInMS(), condition.getRemainingTimeInMS()))
            .until(() -> output.readValuesToList().size() == 0);

    // move time forward
    long twoMinutesMs = ofMinutes(2).toMillis();
    long now = System.currentTimeMillis();
    anOrgUnit.setName("a-different-name");
    orgTopic.pipeInput(parent, anOrgUnit, now + twoMinutesMs);

    // check record emitted
    List<ComponentAggregate> records = output.readValuesToList();
    assertThat(records).hasSize(1);
    ComponentAggregate actual = records.stream().findFirst().get();
    assertThat(actual.getOne()).isEqualTo(anOrgUnit);
    assertThat(actual.getTwo()).isEqualTo(subType);
    assertThat(actual.getThree()).isNull();
  }

  @Test
  void suppressionWindowLookupStreamTime() {
    test(aggTopicClosses);
  }

  @Test
  @Disabled
  void customWallClockTimer() {
    test(aggTopicCustom);
  }

}
