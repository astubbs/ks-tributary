package io.confluent.ps.streams.referenceapp.joinspeed;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyParent;
import io.confluent.ps.streams.referenceapp.joinspeed.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinSpeedTest {

    final String SCHEMA_REGISTRY_SCOPE = SnapshotTopologyParent.class.getName();
    final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

    @Test
    void testBasics() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        new JoinTopology(streamsBuilder);
        KSUtils ksUtils = new KSUtils();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "jointest-" + Math.random());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KSUtils.MOCK_SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());

        try (TopologyTestDriver ttd = new TopologyTestDriver(streamsBuilder.build(), config);) {

            TestInputTopic<TeamId, TeamFollowers> teamFollowers = ttd.createInputTopic(JoinTopology.TEAM_FOLLOWERS,
                    ksUtils.<TeamId>serdeFor(true).serializer(), ksUtils.<TeamFollowers>serdeFor().serializer());
            TestInputTopic<TeamId, Event> gameEvents = ttd.createInputTopic(JoinTopology.GAME_EVENTS,
                    ksUtils.<TeamId>serdeFor(true).serializer(), ksUtils.<Event>serdeFor().serializer());
            TestInputTopic<UserId, UserDeviceToken> deviceTokens = ttd.createInputTopic(JoinTopology.DEVICE_TOKENS,
                    ksUtils.<UserId>serdeFor(true).serializer(), ksUtils.<UserDeviceToken>serdeFor().serializer());

            TestOutputTopic<UserId, EventFollowerDevice> payloadOut = ttd.createOutputTopic(JoinTopology.PAYLOAD_READY_TOPIC,
                    ksUtils.<UserId>serdeFor(true).deserializer(), ksUtils.<EventFollowerDevice>serdeFor().deserializer());

            // data setup
            TeamId t1 = new TeamId("t1");
            UserId u1 = new UserId("u1");
            teamFollowers.pipeInput(t1, new TeamFollowers(t1, List.of(u1)));
            DeviceToken device1 = new DeviceToken("device1");
            deviceTokens.pipeInput(u1, new UserDeviceToken(u1, device1));

            // a game event
            gameEvents.pipeInput(t1, new Event(new EventId("e1"), t1, "one"));

            // output
            List<EventFollowerDevice> eventFollowerDevices = payloadOut.readValuesToList();

            assertThat(eventFollowerDevices).isNotEmpty().hasSize(1).hasOnlyOneElementSatisfying(x -> {
                assertThat(x.getTeamId()).isEqualTo(t1);
                assertThat(x.getUserId()).isEqualTo(u1);
                DeviceToken token = x.getToken();
                assertThat(token.getDeviceToken()).isEqualTo(device1.getDeviceToken());
            });
        }

    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> serdeFor(boolean isKey) {
        SpecificAvroSerde<T> tSpecifivAvroSerde = new SpecificAvroSerde<T>();
        tSpecifivAvroSerde.configure(serdeConfig, isKey);
        return tSpecifivAvroSerde;
    }

}
