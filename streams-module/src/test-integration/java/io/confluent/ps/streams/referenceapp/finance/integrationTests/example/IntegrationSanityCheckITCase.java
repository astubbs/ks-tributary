package io.confluent.ps.streams.referenceapp.finance.integrationTests.example;

import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.integrationTests.KafkaTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;

@Slf4j
@Testcontainers
public class IntegrationSanityCheckITCase extends KafkaTest<InstrumentId, InstrumentId> {

  @Test
  @DisplayName("kafka server should be running")
  void shouldBeRunningKafka() throws Exception {
    assertThat(kafkaContainer).isNotNull().extracting(x -> x.isRunning()).isEqualTo(true);
  }

  @Test
  @SneakyThrows
  void canProduce() {
    produce("canProduce");
  }

  private KeyValue<RecordMetadata, ProducerRecord<InstrumentId, InstrumentId>> produce(String topicName) throws InterruptedException, ExecutionException {
    InstrumentId build = InstrumentId.newBuilder().setId("ID" + RandomStringUtils.randomAscii(5)).build();
    ProducerRecord<InstrumentId, InstrumentId> id = new ProducerRecord<>(topicName, build);
    RecordMetadata recordMetadata = producer.send(id).get();
    assertThat(recordMetadata).isNotNull();
    log.debug("Sent: " + id);
    return KeyValue.pair(recordMetadata, id);
  }

  @Test
  @SneakyThrows
  void canConsume() {
    String topicName = "canConsume-" + RandomUtils.nextInt(0, 1000);

    KeyValue<RecordMetadata, ProducerRecord<InstrumentId, InstrumentId>> produce = produce(topicName);

    consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("off: {}", partitions);
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("on: {}", partitions);
      }
    });


    final List<ConsumerRecord<InstrumentId, InstrumentId>> results = Lists.newArrayList();
    await().atMost(ofSeconds(15)).until(() -> {
      log.info("poll main");
      ConsumerRecords<InstrumentId, InstrumentId> records = consumer.poll(ofMillis(0));
      consumer.commitSync();
      records.forEach(record -> {
        results.add(record);
        log.debug("offset: {}, value = {}", record.offset(),
                record.value());
      });
      return records;
    }, iterableWithSize(greaterThan(0)));

    assertThat(results).hasSize(1);
    assertThat(results.get(0))
            .extracting(x -> x.value())
            .isInstanceOf(InstrumentId.class)
            .isEqualTo(produce.value.value());
  }

  @SneakyThrows
  @Test
  @DisplayName("should send and receive records over kafka")
  void shouldSendAndReceiveMessages() {
    String topicName = "sendAndReceive-" + RandomUtils.nextInt(0, 1000);

    int target = 20000;
    var counter = new AtomicInteger(0);

    Runnable con = () -> {
      try {
        consumer.subscribe(Arrays.asList(topicName));
        while (counter.get() < target) {
          ConsumerRecords<InstrumentId, InstrumentId> records = consumer.poll(ofMillis(200));
          records.forEach(record -> {
            int i = counter.incrementAndGet();
//            log.debug("Consumed: {} # offset: {}, value = {}", i, record.offset(), record.value());
          });
        }
      } catch (Exception e) {
        log.warn("Con interrupted");
      }
    };

    Runnable prod = () -> {
      try {
        IntStream.range(0, target).forEach(i -> {
          var msg = String.format("my-message-%d", i);
          ProducerRecord<InstrumentId, InstrumentId> rec = new ProducerRecord<>(topicName, InstrumentId.newBuilder().setId("ID").build(), InstrumentId.newBuilder().setId(msg).build());
          producer.send(rec);
//          log.debug("Produced: {}", msg);
        });
      } catch (Exception e) {
        log.warn("Prod interrupted");
      }
    };

    ExecutorService e = Executors.newFixedThreadPool(2);
    List<Future> futures = List.of(con, prod).stream().map(e::submit).collect(Collectors.toUnmodifiableList());
    log.debug("after");

    try {
      await().atMost(ofSeconds(20)).until(() -> futures.stream().allMatch(Future::isDone));
    } catch (ConditionTimeoutException t) {
      t.printStackTrace();
      e.shutdownNow();
      e.awaitTermination(5, SECONDS);
      assertThat(t).doesNotThrowAnyException();
    } finally {
      e.shutdown();
    }

  }


}
