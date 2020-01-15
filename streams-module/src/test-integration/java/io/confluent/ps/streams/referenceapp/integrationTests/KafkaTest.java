package io.confluent.ps.streams.referenceapp.integrationTests;

import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.Properties;

@Slf4j
public abstract class KafkaTest<K, V> {

  @Container
  static public KafkaContainer kafkaContainer = new KafkaContainer("5.3.1");
//  public KafkaContainer kafkaContainer;

  protected Properties props = new Properties();

  protected KafkaConsumer<K, V> consumer;

  protected KafkaProducer<K, V> producer;

  @BeforeEach
  public void inject() {
    String servers = kafkaContainer.getBootstrapServers();

    props.put("bootstrap.servers", servers);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KSUtils.MOCK_SCHEMA_REGISTRY_URL);
    props.put("key.serializer", SpecificAvroSerializer.class.getName());
    props.put("value.serializer", SpecificAvroSerializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
    props.put("key.deserializer", SpecificAvroDeserializer.class.getName());
    props.put("value.deserializer", SpecificAvroDeserializer.class.getName());
//    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10);
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 100);
  }

  @BeforeEach
  void open() {
    this.consumer = this.createNewConsumer();
    this.producer = this.createNewProducer();
  }

  @AfterEach
  void close() {
    this.producer.close();
    this.consumer.close();
  }


  private <K, V> KafkaConsumer<K, V> createNewConsumer() {
    KafkaConsumer<K, V> kvKafkaConsumer = new KafkaConsumer<>(props);
    log.debug("New consume {}", kvKafkaConsumer);
    return kvKafkaConsumer;
  }

  private <K, V> KafkaProducer<K, V> createNewProducer() {
    KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(props);
    log.debug("New producer {}", kvKafkaProducer);
    return kvKafkaProducer;
  }


}
