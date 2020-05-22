package io.confluent.ps.streams.referenceapp.chat;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import com.google.common.collect.Lists;
import io.confluent.kafka.streams.serdes.avro.WrappingSpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.chat.model.*;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
public class ChatKpiTest extends GuiceInjectedTestBase {

  @Inject
  TestDataDriver tdd;

  @Inject
  TopologyTestDriver td;

  @Inject
  KSUtils ksutils;

  TestInputTopic<GoalId, GoalEventWrapped> goalsTopic;

  @BeforeEach
  void setup() {
    WrappingSpecificAvroSerde<GoalId, GoalId> keySerde = ksutils.<GoalId, GoalId>wrappingSerdeFor(true);
    WrappingSpecificAvroSerde<GoalEventWrapped, GoalEvent> valueSerde = ksutils.<GoalEvent, GoalEventWrapped>wrappingSerdeFor(false);

    goalsTopic = td.createInputTopic(ChatKpiTopology.GOAL_EVENTS_TOPIC, keySerde.serializer(), valueSerde.serializer());
  }

  @Test
  void goalProgressCountTest() {
    GoalId gIda = GoalId.newBuilder().setId("g1").build();
    GoalId gIdThree = GoalId.newBuilder().setId("g3").build();

    UserId su1 = UserId.newBuilder().setId("su1").build();
    GoalEventWrapped gEvent = newGoal(gIda, su1);

    GoalId gIdb = GoalId.newBuilder().setId("g2").build();
    UserId su2 = UserId.newBuilder().setId("su2").build();
    GoalEventWrapped gEvent2 = newGoal(gIdb, su2);

    goalsTopic.pipeInput(gIda, gEvent);
    goalsTopic.pipeInput(gIdb, gEvent2);

    KeyValueStore<UserId, Long> progressed = td.getKeyValueStore(ChatKpiTopology.PROGRESSED_COUNT_STORE);
    KeyValueStore<UserId, Long> veryProgressed = td.getKeyValueStore(ChatKpiTopology.VERY_PROGRESSED_COUNT_STORE);
    KeyValueStore<Object, Object> progressedGoalCounts = td.getKeyValueStore(ChatKpiTopology.PROGRESSED_GOALS_STORE);
    KeyValueStore<UserId, Long> progressedCountPerUsers = td.getKeyValueStore(ChatKpiTopology.GOALS_PROGRESSED_PER_USER_T2);


    ArrayList<KeyValue<UserId, Long>> all = Lists.newArrayList(progressed.all());
    ArrayList<KeyValue<UserId, Long>> allv = Lists.newArrayList(veryProgressed.all());

    // no progress
    assertThat(all).isEmpty();
    assertThat(allv).isEmpty();

    // advance goal event
    goalsTopic.pipeInput(gIda, new GoalEventWrapped(gIda, "my goal", 2, 0, DateTime.now(), su1, ReviewedStatus.YES));

    // test
    assertThat(Lists.newArrayList(progressed.all())).hasSize(1);
    assertThat(Lists.newArrayList(progressed.all()).get(0).value).isEqualTo(1l);
    assertThat(Lists.newArrayList(veryProgressed.all())).hasSize(0);

    assertThat(Lists.newArrayList(progressedGoalCounts.all())).hasSize(1);

    // advance goal even further
    goalsTopic.pipeInput(gIda, new GoalEventWrapped(gIda, "my goal", 3, 0, DateTime.now(), su1, ReviewedStatus.YES));

    // test
    assertThat(Lists.newArrayList(progressed.all())).hasSize(1);
    assertThat(Lists.newArrayList(progressed.all()).get(0).value).isEqualTo(2l); // counts number of goal progression events
    assertThat(Lists.newArrayList(veryProgressed.all())).hasSize(1);

    // add another goal
    goalsTopic.pipeInput(gIdThree, new GoalEventWrapped(gIdThree, "my goal", 0, 0, DateTime.now(), su1, ReviewedStatus.YES));
    goalsTopic.pipeInput(gIdThree, new GoalEventWrapped(gIdThree, "my goal", 5, 0, DateTime.now(), su1, ReviewedStatus.YES));

    // test
    ArrayList<KeyValue<Object, Object>> actual = Lists.newArrayList(progressedGoalCounts.all());
    assertThat(actual).hasSize(2);

    // progress further
    goalsTopic.pipeInput(gIdThree, new GoalEventWrapped(gIdThree, "my goal", 9, 0, DateTime.now(), su1, ReviewedStatus.YES));

    // test there's still only two progressed goals
    assertThat(Lists.newArrayList(progressedGoalCounts.all())).hasSize(2);

    // test
    assertThat(Lists.newArrayList(progressedCountPerUsers.all())).hasSize(1);
    assertThat(progressedCountPerUsers.get(su1)).as("test the count of progressed for user").isEqualTo(2l);

    // progress another users goal
    goalsTopic.pipeInput(gIdb, new GoalEventWrapped(gIdb, "my goal", 9, 0, DateTime.now(), su2, ReviewedStatus.YES));
    assertThat(Lists.newArrayList(progressedCountPerUsers.all())).hasSize(2);
    assertThat(progressedCountPerUsers.get(su2)).isEqualTo(1l);
  }

  @NotNull
  private GoalEventWrapped newGoal(GoalId goalId, UserId suId) {
    return newGoal(goalId, suId, 0);
  }

  @NotNull
  private GoalEventWrapped newGoal(GoalId goalId, UserId suId, int score) {
    return new GoalEventWrapped(goalId, "my goal", 0, 0, DateTime.now(), suId, ReviewedStatus.NO);
  }

}
