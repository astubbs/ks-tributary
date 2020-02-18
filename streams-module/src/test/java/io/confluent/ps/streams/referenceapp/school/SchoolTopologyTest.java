package io.confluent.ps.streams.referenceapp.school;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.schools.model.OrgUnit;
import io.confluent.ps.streams.referenceapp.schools.model.SchStatusCode;
import io.confluent.ps.streams.referenceapp.schools.model.SchSubtype;
import io.confluent.ps.streams.referenceapp.schools.model.SchoolId;
import io.confluent.ps.streams.referenceapp.schools.topology.SchoolTopology;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

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

  @BeforeEach
  void setup() {
//    var keySerde = ksutils.<GoalId, GoalId>wrappingSerdeFor(true);
//    var valueSerde = ksutils.<GoalEvent, GoalEventWrapped>wrappingSerdeFor(false);
    var keySerde = ksutils.<SchoolId>serdeFor(true);
    var orgSerde = ksutils.<OrgUnit>serdeFor(false);
    var statusSerde = ksutils.<SchStatusCode>serdeFor(false);
    var subtypeSerde = ksutils.<SchSubtype>serdeFor(false);

    orgTopic = td.createInputTopic(SchoolTopology.ORG_TOPIC, keySerde.serializer(), orgSerde.serializer());
    statusTopic = td.createInputTopic(SchoolTopology.STATUS_CODE_TOPIC, keySerde.serializer(), statusSerde.serializer());
    subtypeTopic = td.createInputTopic(SchoolTopology.SUP_TYPE_TOPIC, keySerde.serializer(), subtypeSerde.serializer());

    td.createOutputTopic(SchoolTopology.AGGREGATE_UPDATES_TOPIC, keySerde.deserializer(), keySerde.deserializer());
  }

  @Test
  void dataAggregator() {
    val schoolId = SchoolId.newBuilder().setId("a-school").build();
    val build = OrgUnit.newBuilder().setCode("code").setSchoolCode().setName("").build();
    orgTopic.pipeInput(schoolId, build);
  }

}
