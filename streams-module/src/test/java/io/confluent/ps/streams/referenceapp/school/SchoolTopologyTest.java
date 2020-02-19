package io.confluent.ps.streams.referenceapp.school;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.schools.model.*;
import io.confluent.ps.streams.referenceapp.schools.topology.SchoolTopology;
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

import static io.confluent.ps.streams.referenceapp.schools.topology.SchoolTopology.suppressionWindowTime;
import static java.time.Duration.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
public class SchoolTopologyTest extends GuiceInjectedTestBase {

  @Inject
  TestDataDriver tdd;

  @Inject
  TopologyTestDriver td;

  @Inject
  KSUtils ksutils;

  TestInputTopic<SchoolId, OrgUnit> orgTopic;
  TestInputTopic<SchoolId, SchStatusCode> statusTopic;
  TestInputTopic<SchoolId, SchSubtype> subtypeTopic;
  private TestOutputTopic<SchoolId, SchoolAggregate> aggTopic;

  @BeforeEach
  void setup() {
//    var keySerde = ksutils.<GoalId, GoalId>wrappingSerdeFor(true);
//    var valueSerde = ksutils.<GoalEvent, GoalEventWrapped>wrappingSerdeFor(false);
    var keySerde = ksutils.<SchoolId>serdeFor(true);
    var orgSerde = ksutils.<OrgUnit>serdeFor(false);
    var statusSerde = ksutils.<SchStatusCode>serdeFor(false);
    var subtypeSerde = ksutils.<SchSubtype>serdeFor(false);
    var aggSerde = ksutils.<SchoolAggregate>serdeFor(false);

    orgTopic = td.createInputTopic(SchoolTopology.ORG_UNIT_TOPIC, keySerde.serializer(), orgSerde.serializer());
    statusTopic = td.createInputTopic(SchoolTopology.STATUS_CODE_TOPIC, keySerde.serializer(), statusSerde.serializer());
    subtypeTopic = td.createInputTopic(SchoolTopology.SUP_TYPE_TOPIC, keySerde.serializer(), subtypeSerde.serializer());

    aggTopic = td.createOutputTopic(SchoolTopology.AGGREGATE_UPDATES_TOPIC, keySerde.deserializer(), aggSerde.deserializer());
  }

  @Test
  void dataAggregatorSimpleSuppressUntilStreamTime() {
    // send through some data
    val schoolId = SchoolId.newBuilder().setId("a-school").build();
    val anOrgUnit = OrgUnit.newBuilder().setCode("code").setSchoolCode(schoolId).setName("sdsd").build();
    SchSubtype subType = SchSubtype.newBuilder().setSchoolCode(schoolId).setCode("code").setName("name").build();

    orgTopic.pipeInput(schoolId, anOrgUnit);
    subtypeTopic.pipeInput(schoolId, subType);

    // check records are suppressed
    assertThat(aggTopic.readValuesToList()).hasSize(0);

    // test nothing is emitted, beyond the suppression window, unless we send messages
    // (this slows the test down a "lot")
    //    Awaitility.setDefaultTimeout(ofSeconds(15));
    Duration waitUntil = suppressionWindowTime.plus(ofMillis(500));
    log.debug("Start testing lack of messages (suppression window is {} and we will wait up to {})...", suppressionWindowTime, waitUntil);
    await().during(waitUntil)
//            .pollInterval(ofSeconds(1))
            .conditionEvaluationListener(condition -> log.debug("Still no message emitted... (elapsed:{}ms, remaining:{}ms)", condition.getElapsedTimeInMS(), condition.getRemainingTimeInMS()))
            .until(() -> aggTopic.readValuesToList().size() == 0);

    // move time forward
    long twoMinutesMs = ofMinutes(2).toMillis();
    long now = System.currentTimeMillis();
    orgTopic.pipeInput(schoolId, anOrgUnit, now + twoMinutesMs);

    // check record emitted
    List<SchoolAggregate> records = aggTopic.readValuesToList();
    assertThat(records).hasSize(1);
    SchoolAggregate actual = records.stream().findFirst().get();
    assertThat(actual.getOrg()).isEqualTo(anOrgUnit);
    assertThat(actual.getType()).isEqualTo(subType);
    assertThat(actual.getStatusCode()).isNull();
  }

  @Test
  @Disabled
  void suppressionWindowLookupStreamTime() {
  }

  @Test
  @Disabled
  void customWallClockTimer() {
  }

}
