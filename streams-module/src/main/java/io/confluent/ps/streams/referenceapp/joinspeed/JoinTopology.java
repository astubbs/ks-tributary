package io.confluent.ps.streams.referenceapp.joinspeed;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.joinspeed.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.StateStore;

import java.util.ArrayList;
import java.util.List;

public class JoinTopology {

    public static final String GAME_EVENTS = "game-events";
    public static final String TEAM_FOLLOWERS = "team-followers";
    public static final String DEVICE_TOKENS = "device-tokens";
    public static final String PAYLOAD_READY_TOPIC = "payload-ready-topic";

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

        KTable<UserId, UserDeviceToken> userDeviceTokensTable = builder.table(DEVICE_TOKENS);

        // join user events to device tokens
        KStream<UserId, EventFollowerDevice> joinTokens = rekeyedToUsers.join(userDeviceTokensTable,
                (EventFollower ef, UserDeviceToken udt) ->
                        new EventFollowerDevice(ef.getTeamId(), ef.getUserId(), udt.getToken()));

        joinTokens.to(PAYLOAD_READY_TOPIC);

        // now can optionally use PC to consume payload-ready-topic in parallel
    }

}
