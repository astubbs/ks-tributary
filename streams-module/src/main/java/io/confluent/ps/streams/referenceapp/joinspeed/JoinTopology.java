package io.confluent.ps.streams.referenceapp.joinspeed;

import io.confluent.ps.streams.referenceapp.joinspeed.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JoinTopology {

    public static final String GAME_EVENTS = "game-events";
    public static final String TEAM_FOLLOWERS = "team-followers";
    public static final String DEVICE_TOKENS = "device-tokens";
    public static final String PAYLOAD_READY_TOPIC = "payload-ready-topic";
    public static final String USER_DEVICE_TOKEN_STORE = "user-device-token-store";
    public static final String USER_EVENTS = "user-events";

    public JoinTopology(StreamsBuilder builder) {
        KSUtils ksUtils = new KSUtils();
        KStream<TeamId, Event> events = builder.stream(GAME_EVENTS, Consumed.with(ksUtils.serdeFor(true), ksUtils.serdeFor(true)));
        KTable<TeamId, TeamFollowers> teamFollowersTable = builder.table(TEAM_FOLLOWERS, Consumed.with(ksUtils.serdeFor(true), ksUtils.serdeFor(true)));

        // join event to people following that team
        KStream<TeamId, EventFollowersBatch> eventFollowersBatch = events.join(teamFollowersTable, (event, tf) ->
                new EventFollowersBatch(event.getTeamId(), tf.getUsers()));

        // expand out the team -> user1,user2,user3 to one message per user: team->user1, team->user2
        KStream<TeamId, EventFollower> eventFollowers = eventFollowersBatch.flatMapValues((EventFollowersBatch efb) -> {
            List<UserId> users = efb.getUsers();
            List<EventFollower> result = new ArrayList<>();
            for (final UserId user : users) {
                result.add(new EventFollower(efb.getTeamId(), user));
            }
            return result;
        });

        // rekey (could collapse into previous step if change from flatMapValues to flatMap
        KStream<UserId, EventFollower> rekeyedToUsers = eventFollowers.selectKey((TeamId team, EventFollower ef) ->
                ef.getUserId());

        // send to topic for PC DEMO
        rekeyedToUsers.to(USER_EVENTS);

        // aggregate all user device tokens
        KTable<UserId, UserDeviceTokenRegistry> userDeviceTokensTable = builder.<UserId, DeviceToken>stream(DEVICE_TOKENS).groupByKey().aggregate(
                () -> {
                    UserDeviceTokenRegistry userDeviceTokenRegistry = new UserDeviceTokenRegistry();
                    userDeviceTokenRegistry.setTokens(new ArrayList<>());
                    return userDeviceTokenRegistry;
                },
                (key, additional, agg) -> {
                    agg.getTokens().add(additional);
                    return agg;
                }, Materialized.as(USER_DEVICE_TOKEN_STORE));

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
