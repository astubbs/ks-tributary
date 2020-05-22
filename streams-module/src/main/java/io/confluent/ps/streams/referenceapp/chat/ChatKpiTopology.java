package io.confluent.ps.streams.referenceapp.chat;

import io.confluent.ksql.util.Pair;
import io.confluent.ps.streams.processors.YearlyAggregator;
import io.confluent.ps.streams.referenceapp.chat.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.time.MonthDay;
import java.util.TimeZone;
import java.util.UUID;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMinutes;

public class ChatKpiTopology {

  public static final String GOAL_EVENTS_TOPIC = "goal-events";
  public static final String PROGRESSED_GOALS_TOPIC = "progressed-goals";
  public static final String MUC_ROOM_CHATS_TOPIC = "muc-room-chats";
  public static final String SERVICE_USER_PROFILES_TOPIC = "user-accounts";


  public static final String PROGRESSED_COUNT_STORE = "progressedCount";
  public static final String VERY_PROGRESSED_COUNT_STORE = "veryProgressedCount";
  public static final String PROGRESSED_GOALS_STORE = "progressed-goals-store";
  public static final String GOALS_PROGRESSED_PER_USER_T1 = "goals-progressed-per-user-t1";
  public static final String GOALS_PROGRESSED_PER_USER_T2 = "goals-progressed-per-user-t2";
  public static final String MUC_COUNTER_STORE_NAME = "muc-counter-store";

  private final KSUtils ksutils = new KSUtils();

  @Inject
  public ChatKpiTopology(StreamsBuilder builder) {
    KTable<UserId, UserAccess> serviceUserIdUserAccessKStream = usersWithAccessAchieved(builder);

    goalsKPI(builder, serviceUserIdUserAccessKStream);

    transcriptBuilder(builder);
  }


  private void transcriptBuilder(StreamsBuilder builder) {
    KStream<UserId, ChatMessage> chatMessages = builder.stream("chat-messages");

    KGroupedStream<UserId, ChatMessage> serviceUserIdChatMessageKGroupedStream = chatMessages.groupByKey();

    KTable<UserId, Transcript> transcripts = serviceUserIdChatMessageKGroupedStream
            .aggregate(() -> {
      return Transcript.newBuilder().setId(TranscriptId.newBuilder().setId(UUID.randomUUID().toString()).build()).build();
    }, (key, message, transcript) -> {
      transcript.getMessages().add(message);
      return transcript;
    }, Materialized.as("chat-transcripts-store"));
  }

  /**
   * Users who have achieved access
   *
   * @param builder
   *
   * @return
   */
  private KTable<UserId, UserAccess> usersWithAccessAchieved(StreamsBuilder builder) {
    KTable<UserId, User> serviceUserIdServiceUserProfileKStream = userAccountsTable(builder);

    KStream<UserId, MucRoom> chatStream = mucRoomsStream(builder)
            .selectKey((key, value) -> value.getParticipantOne());
    KStream<UserId, MucRoom> longChats = chatStream
            .filter((userId, chat) -> {
              long end = chat.getEndedAt().getMillis();
              long start = chat.getStartedAt().getMillis();
              boolean chatLastedLongerThan = end - start < ofMinutes(8).toMillis();
      return chatLastedLongerThan;
    });

    KStream<UserId, Pair<MucRoom, User>> longChatsAndUserProfiles = longChats
            .join(serviceUserIdServiceUserProfileKStream, Pair::of);

    // count access per financial
    StoreBuilder mucChatStore = Stores.windowStoreBuilder(Stores.persistentWindowStore(MUC_COUNTER_STORE_NAME,
            ofDays(900),
            ofDays(300),
            false
            ),
            ksutils.serdeFor(true), ksutils.serdeFor(false));
    builder.addStateStore(mucChatStore);

    KStream<UserId, KeyValue<UserId, Integer>> countedStream = longChats
            .transformValues(() -> new YearlyAggregator<>(
                    MonthDay.of(4, 1),
                    TimeZone.getTimeZone("GMT"),
                    () -> 0,
                    (key, value, aggregate) -> aggregate + 1,
                    MUC_COUNTER_STORE_NAME), MUC_COUNTER_STORE_NAME);

    // convert access counts stream to table of user access objects
    KTable<UserId, UserAccess> serviceUserIdUserAccessKTable = countedStream
            .groupByKey()
            .reduce((value1, value2) -> value2)
            .mapValues((readOnlyKey, value) ->
                    UserAccess
                            .newBuilder()
                            .setUserId(readOnlyKey)
                            .setAccessTime(DateTime.now())
                            .setAccessCount(value.value + 1)
                            .build());
    return serviceUserIdUserAccessKTable;
  }

  private KStream<MucRoomId, MucRoom> mucRoomsStream(StreamsBuilder builder) {
    KStream<MucRoomId, MucRoom> stream = builder.stream(MUC_ROOM_CHATS_TOPIC);
    return stream;
  }

  private KTable<UserId, User> userAccountsTable(StreamsBuilder builder) {
    KTable<UserId, User> stream = builder.table(SERVICE_USER_PROFILES_TOPIC);
    return stream;
  }

  private void goalsKPI(StreamsBuilder builder, KTable<UserId, UserAccess> usersWithAccess) {
    KStream<GoalId, GoalEventWrapped> goalEvents = builder.stream(GOAL_EVENTS_TOPIC);
    KStream<GoalId, GoalEventWrapped> progressedGoalsEvents = goalEvents
            .filter((key, goal) -> goal.hasMadeProgress());

    techniqueOneGoalsAnalysis(progressedGoalsEvents, usersWithAccess);

    techniqueTwoGoalsAnalysis(builder, usersWithAccess, progressedGoalsEvents);
  }


  /// GOALS

  private void techniqueTwoGoalsAnalysis(StreamsBuilder builder, KTable<UserId, UserAccess> usersWithAccess, KStream<GoalId, GoalEventWrapped> progressedGoalsEvents) {
    KStream<GoalId, GoalEventWrapped> veryProgressedGoalsEvents = progressedGoalsEvents
            .filter((key, goal) -> goal.hasSignificantProgress());

    // construct cache
    KTable<GoalId, GoalEventWrapped> progressedGoals = builder.table(PROGRESSED_GOALS_TOPIC, Materialized.as(PROGRESSED_GOALS_STORE));

    // check if exists in cache
    progressedGoalsEvents
            .leftJoin(progressedGoals, Pair::of)
            .filter((key, value) -> {
              boolean noMatchingValue = value.right == null; // join miss, first time this goal has progressed
              return noMatchingValue;
            })
            .mapValues(value -> value.left)
            .to(PROGRESSED_GOALS_TOPIC);

    KStream<GoalId, GoalEventWrapped> initialGoalProgressionEvents = progressedGoals.toStream();
    initialGoalProgressionEvents
            .selectKey((key, goalEvent) -> goalEvent.getUserId())
            .groupByKey()
            .count(Materialized.as(GOALS_PROGRESSED_PER_USER_T2));

    // filter by users with access - draft
    // KStream<ServiceUserId, Pair<GoalEventWrapped, UserAccess>> initialGoalProgressionEventsWithAccess = progressedGoals.toStream().selectKey((key, value) -> value.getServiceUserId()).join(usersWithAccess, Pair::of);
    // initialGoalProgressionEventsWithAccess.selectKey((key, goalEvent) -> goalEvent.left.getServiceUserId()).groupByKey().count(Materialized.as(GOALS_PROGRESSED_PER_USER_T2));

    KTable<UserId, Long> progressedEventsCount = progressedGoalsEvents
            .selectKey((key, value) -> value.getUserId())
            .groupByKey()
            .count(Materialized.as(PROGRESSED_COUNT_STORE));
    KTable<UserId, Long> veryProgressedEventsCount = veryProgressedGoalsEvents
            .selectKey((key, value) -> value.getUserId())
            .groupByKey()
            .count(Materialized.as(VERY_PROGRESSED_COUNT_STORE));
  }

  private void techniqueOneGoalsAnalysis(KStream<GoalId, GoalEventWrapped> progressedGoalsEvents, KTable<UserId, UserAccess> usersWithAccess) {
    KTable<GoalId, GoalEventWrapped> stepCountedGoalProgression = progressedGoalsEvents.groupByKey().reduce((value1, value2) -> {
      value2.processStep();
      return value2;
    });

    KStream<UserId, Pair<GoalEventWrapped, UserAccess>> initialGoalProgressionEventsWithAccess = stepCountedGoalProgression
            .toStream()
            .selectKey((key, value) -> value.getUserId())
            .join(usersWithAccess, Pair::of);
    initialGoalProgressionEventsWithAccess.selectKey((key, goalEvent) -> goalEvent.left.getUserId())
            .groupByKey()
            .count(Materialized.as(GOALS_PROGRESSED_PER_USER_T1));

    KTable<GoalId, GoalEventWrapped> firstProgressionStep = stepCountedGoalProgression.filter((key, value) -> value.getGoalProgressionSteps() == 1);
    KTable<GoalId, GoalEventWrapped> firstProgressionStepSignificant = stepCountedGoalProgression.filter((key, value) -> value.isAtSignificantStep());
  }

}
