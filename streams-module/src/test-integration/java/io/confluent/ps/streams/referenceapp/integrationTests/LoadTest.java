package io.confluent.ps.streams.referenceapp.integrationTests;

import com.google.common.collect.Iterables;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen.DataGenModule;
import io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen.TickGenerator;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import name.falgout.jeffrey.testing.junit.guice.GuiceExtension;
import name.falgout.jeffrey.testing.junit.guice.IncludeModule;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IncludeModule(DataGenModule.class)
@ExtendWith(GuiceExtension.class)
@Testcontainers
@Slf4j
public class LoadTest extends KafkaTest<InstrumentId, InstrumentTick> {

  @Inject
  TickGenerator gen;

  @Inject
  List<InstrumentId> ids;

  @Test
  void timedLoadItUp() {
    assertThat(kafkaContainer.isRunning()).isTrue();

    String topic = LoadTest.class.getName() + RandomUtils.nextInt(0, 1000);

    // subscribe in advance, it can be a few seconds
    consumer.subscribe(List.of(topic));

    int total = 500;

    // produce ticks
    IntStream ints = IntStream.range(0, total);
    ArrayList<Integer> integers = Lists.newArrayList(ints.iterator());

    Iterable<Integer> taskName = ProgressBar.wrap(integers, "Publish");
    for (Integer x : taskName) {
      List<InstrumentTick> ticks = gen.constructTicks(1, ids);
      ticks.forEach(t -> {
        ProducerRecord<InstrumentId, InstrumentTick> producerRecord = new ProducerRecord<InstrumentId, InstrumentTick>(topic, t.getId(), t);
        try {
          RecordMetadata recordMetadata = producer.send(producerRecord).get();

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    final List<ConsumerRecord<InstrumentId, InstrumentTick>> allRecords = Lists.newArrayList();
    Duration timeAllowedToTakeToConsume = ofSeconds(15);
    try (ProgressBar pb = new ProgressBar("Read", total)) {
      await().atMost(timeAllowedToTakeToConsume).untilAsserted(() -> {


        ConsumerRecords<InstrumentId, InstrumentTick> poll = consumer.poll(ofMillis(200));
        Iterable<ConsumerRecord<InstrumentId, InstrumentTick>> records = poll.records(topic);
        Iterables.addAll(allRecords, records);
//      poll.forEach(x -> {
//        log.debug(x.toString());
//      });

        assertThat(allRecords).hasSize(total);


      });
      pb.stepTo(allRecords.size());
    }
    assertThat(allRecords).isNotEmpty();
  }

  @Test
  @Disabled
  void runKSAppAgainstKafka() {
  }

}
