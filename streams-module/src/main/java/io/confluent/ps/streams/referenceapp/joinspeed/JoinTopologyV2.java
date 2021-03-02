package io.confluent.ps.streams.referenceapp.joinspeed;

import com.google.common.collect.BoundType;
import io.confluent.ps.streams.referenceapp.joinspeed.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JoinTopologyV2 {

    public static final String GAME_EVENTS = "game-events";
    public static final String DEVICE_TOKENS = "device-tokens";
    public static final String PAYLOAD_READY_TOPIC = "payload-ready-topic";
    public static final String USER_DEVICE_TOKEN_STORE = "user-device-token-store";
    public static final String USER_EVENTS = "user-events";
    public static final int NUM_TEAM_FOLLOWER_SHARDS = 2;
    public static final String USER_FOLLOWS_TEAM_EVENT = "user-follows-team-event";
    public static final String TEAM_SHARDS = "team-shards";

    public JoinTopologyV2(StreamsBuilder builder) {
        KSUtils ksUtils = new KSUtils();
        KStream<TeamId, Event> events = builder.stream(GAME_EVENTS, Consumed.with(ksUtils.serdeFor(true), ksUtils.serdeFor(true)));

        KStream<UserId, TeamId> userFollowsTeamEvents = builder.stream(USER_FOLLOWS_TEAM_EVENT, Consumed.with(ksUtils.serdeFor(true), ksUtils.serdeFor(true)));


        // 1. prepare table

        // asign user team follower to a team follower shard
        KStream<TeamFollowerShardKey, UserId> teamFollowerShardKeyTeamIdKStream = userFollowsTeamEvents.map((userId, teamId) -> {
            int followerShard = userId.getUserId().hashCode() % NUM_TEAM_FOLLOWER_SHARDS;
            return new KeyValue<>(new TeamFollowerShardKey(teamId, followerShard), userId);
        });

        // aggregate to team followers table, by followers shard
        KTable<TeamFollowerShardKey, TeamFollowers> teamFollowersTable = teamFollowerShardKeyTeamIdKStream.groupByKey().aggregate(
                () -> {
                    TeamFollowers teamFollowers = new TeamFollowers();
                    teamFollowers.setUsers(new ArrayList<>());
                    teamFollowers.setTeamId(new TeamId(""));
                    return teamFollowers;
                },
                (key, user, agg) -> {
                    agg.setTeamId(key.getTeamId());
                    agg.getUsers().add(user);
                    return agg;
                }, Materialized.as(TEAM_SHARDS));


        // 2. join to events
        // fan out to all NUM_TEAM_FOLLOWER_SHARDS
        KStream<TeamFollowerShardKey, Event> teamFollowerShardKeyEventKStream = events.flatMap((teamId, event) -> {
            List<KeyValue<TeamFollowerShardKey, Event>> result = new ArrayList<>();
            for (int i = 0; i < NUM_TEAM_FOLLOWER_SHARDS; i++) {
                result.add(KeyValue.pair(new TeamFollowerShardKey(teamId, i), event));
            }
            return result;
        });

        // join event to people following that team
        KStream<TeamFollowerShardKey, EventFollowersBatch> eventFollowersBatch = teamFollowerShardKeyEventKStream.join(teamFollowersTable, (event, tf) ->
                new EventFollowersBatch(event.getTeamId(), tf.getUsers()));

        // expand out the team -> user1,user2,user3 to one message per user: team->user1, team->user2
        KStream<TeamId, EventFollower> eventFollowers = eventFollowersBatch.flatMap((TeamFollowerShardKey tfsk, EventFollowersBatch efb) -> {
            List<UserId> users = efb.getUsers();
            List<KeyValue<TeamId, EventFollower>> result = new ArrayList<>();
            for (final UserId user : users) {
                result.add(KeyValue.pair(tfsk.getTeamId(), new EventFollower(efb.getTeamId(), user)));
            }
            return result;
        });

        // rekey (could collapse into previous step if change from flatMapValues to flatMap
        KStream<UserId, EventFollower> rekeyedToUsers = eventFollowers.selectKey((TeamId team, EventFollower ef) ->
                ef.getUserId());

        // send to topic for PC DEMO
        rekeyedToUsers.to(USER_EVENTS);

        // aggregate all user device tokens
        KTable<UserId, UserDeviceTokenRegistry> userDeviceTokensTable = builder.<UserId, DeviceToken>stream(DEVICE_TOKENS)
                .groupByKey()
                .aggregate(
                        () -> {
                            UserDeviceTokenRegistry userDeviceTokenRegistry = new UserDeviceTokenRegistry();
                            userDeviceTokenRegistry.setTokens(new ArrayList<>());
                            return userDeviceTokenRegistry;
                        },
                        (key, additional, agg) -> {
                            agg.getTokens().add(additional);
                            return agg;
                        }, Materialized.as(USER_DEVICE_TOKEN_STORE));

        // publish derived KTable for global KTable (no need to materialise the above in this case)
        userDeviceTokensTable.toStream().to(USER_DEVICE_TOKEN_STORE + "-global");
        String storeName = builder.globalTable("", Materialized.as(USER_DEVICE_TOKEN_STORE + "-global-store")).queryableStoreName();

        // join user events to device tokens
        KStream<UserId, EventFollowerDevice> joinTokens = rekeyedToUsers.join(userDeviceTokensTable,
                (EventFollower ef, UserDeviceTokenRegistry udtr) ->
                        new EventFollowerDeviceAggregate(ef.getTeamId(), ef.getUserId(), udtr.getTokens()))

                // fan out to per device per user
                .flatMapValues((EventFollowerDeviceAggregate agg) -> agg.getTokens().stream()
                        .map(x -> new EventFollowerDevice(agg.getTeamId(), agg.getUserId(), x))
                        .collect(Collectors.toList()));

        // now can optionally use PC to consume payload-ready-topic in parallel
        joinTokens.to(PAYLOAD_READY_TOPIC);
    }

}
