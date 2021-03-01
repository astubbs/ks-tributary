package io.confluent.ps.streams.referenceapp.joinspeed;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyParent;
import io.confluent.ps.streams.referenceapp.joinspeed.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.assertj.core.presentation.StandardRepresentation;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.ps.streams.referenceapp.joinspeed.JoinTopology.USER_DEVICE_TOKEN_STORE;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
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
            TestInputTopic<UserId, DeviceToken> deviceTokens = ttd.createInputTopic(JoinTopology.DEVICE_TOKENS,
                    ksUtils.<UserId>serdeFor(true).serializer(), ksUtils.<DeviceToken>serdeFor().serializer());

            TestOutputTopic<UserId, EventFollowerDevice> payloadOut = ttd.createOutputTopic(JoinTopology.PAYLOAD_READY_TOPIC,
                    ksUtils.<UserId>serdeFor(true).deserializer(), ksUtils.<EventFollowerDevice>serdeFor().deserializer());

            // data setup
            TeamId t1 = new TeamId("t1");

            UserId u1 = new UserId("u1");
            UserId u2 = new UserId("u2");
            UserId u3 = new UserId("u3");

            UserId u4 = new UserId("u4"); // multi device user

            teamFollowers.pipeInput(t1, new TeamFollowers(t1, List.of(u1, u2, u3, u4)));

            DeviceToken device1 = new DeviceToken("device1");
            DeviceToken device2 = new DeviceToken("device2");
            DeviceToken device3 = new DeviceToken("device3");

            DeviceToken device4 = new DeviceToken("device4");
            DeviceToken device5 = new DeviceToken("device5");
            DeviceToken device6 = new DeviceToken("device6");

            deviceTokens.pipeInput(u1, device1);
            deviceTokens.pipeInput(u2, device2);
            deviceTokens.pipeInput(u3, device3);

            deviceTokens.pipeInput(u4, device4);
            deviceTokens.pipeInput(u4, device5);
            deviceTokens.pipeInput(u4, device6);


            // a game event
            gameEvents.pipeInput(t1, new Event(new EventId("e1"), t1, "one"));

            // output
            List<KeyValue<UserId, EventFollowerDevice>> eventFollowerDevices = payloadOut.readKeyValuesToList();

            assertThat(eventFollowerDevices)
                    .isNotEmpty()
                    .hasSize(6);

            log.info("\n" + new StandardRepresentation().toStringOf(eventFollowerDevices));


            // DEMO query of state store
            KeyValueStore devices = ttd.getKeyValueStore(USER_DEVICE_TOKEN_STORE);
            new ParallelJoin(devices, new ParallelEoSStreamProcessor<>(null)); // needs options setup

        }

    }

    /**
     * PC Join Demo - stream of events
     */
    class ParallelJoin {

        /**
         * Needs KeyValueStore injected.
         *
         * E.g. KeyValueStore devices = ttd.getKeyValueStore(USER_DEVICE_TOKEN_STORE);
         */
        ParallelJoin(KeyValueStore<UserId, UserDeviceTokenRegistry> store, ParallelConsumer<UserId, EventFollower> pc) {
            pc.poll(record -> {
                UserId userId = record.key();
                EventFollower value = record.value();

                UserDeviceTokenRegistry userDeviceTokenRegistry = store.get(userId);
                if (userDeviceTokenRegistry != null) {
                    // join hit
                    // create payload with even details and call third party system, or produce a result message
                    value.getTeamId();
                    //....
                } else {
                    // join miss
                    // drop - not registered devices for that user
                }
            });
        }

    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> serdeFor(boolean isKey) {
        SpecificAvroSerde<T> tSpecifivAvroSerde = new SpecificAvroSerde<T>();
        tSpecifivAvroSerde.configure(serdeConfig, isKey);
        return tSpecifivAvroSerde;
    }

}
