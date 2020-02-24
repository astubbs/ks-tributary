package io.confluent.ps.streams.referenceapp.denormilsation.topology;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.streams.referenceapp.denormalisation.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.val;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import javax.inject.Inject;
import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class DenormalisationTopology {

  final KSUtils ksUtils;

  public static final String COMP_A_TOPIC = "comp_a";
  public static final String COMP_B_TOPIC = "com_b";
  public static final String COMP_C_TOPIC = "com_c";
  public static final String AGGREGATE_UPDATES_TOPIC_SUPPRESS_UNTIL = "aggregate_updates_until";
  public static final String AGGREGATE_UPDATES_TOPIC_SUPPRESS_CLOSES = "aggregate_updates_closes";
  public static final String AGGREGATE_UPDATES_TOPIC_CUSTOM = "aggregate_updates_custom";
  public static final String SCHOOL_AGGREGATE_STORE = "school-aggregate-store";
  public static final String SCHOOL_AGGREGATE_SUPPRESSION_STORE = "school-aggregate-suppression-store";

  public static final Duration suppressionWindowTime = Duration.ofMillis(500); // for testing

  private final StoreProvider storeProvider;

  @Inject
  public DenormalisationTopology(StreamsBuilder builder, StoreProvider storeProvider, KSUtils ksUtils) {
    this.storeProvider = storeProvider;
    this.ksUtils = ksUtils;

    buildTopology(builder);
  }

  private void buildTopology(StreamsBuilder builder) {
    // must use ConsumedWith because KAFKA-9259 suppress() for windowed-Serdes does not work with default serdes
    SpecificAvroSerde<DocumentId> keySerde = ksUtils.serdeFor();
    SpecificAvroSerde<SpecificRecord> valueSerde = ksUtils.serdeFor();
    Consumed<DocumentId, SpecificRecord> with = Consumed.with(keySerde, valueSerde);

    KStream<DocumentId, SpecificRecord> orgTopicStream = builder.stream(COMP_A_TOPIC, with)
            .selectKey((k, v) -> ((ComponentOne) v).getParentId());
    KStream<DocumentId, SpecificRecord> subTypeStream = builder.stream(COMP_B_TOPIC, with)
            .selectKey((k, v) -> ((ComponentTwo) v).getParentId());
    KStream<DocumentId, SpecificRecord> statusCodeStream = builder.stream(COMP_C_TOPIC, with)
            .selectKey((k, v) -> ((ComponentThree) v).getParentId());

    KStream<DocumentId, SpecificRecord> mergedStream = orgTopicStream
            .merge(subTypeStream)
            .merge(statusCodeStream);

    KTable<DocumentId, ComponentAggregate> aggregate = mergedStream
            .groupByKey()
            .aggregate(ComponentAggregate::new, (key, value, componentAggregate) -> {

              componentAggregate.setParentId(key); // yuck

              if (value instanceof ComponentOne) {
                componentAggregate.setOne((ComponentOne) value);
              } else if (value instanceof ComponentTwo) {
                componentAggregate.setTwo((ComponentTwo) value);
              } else if (value instanceof ComponentThree) {
                componentAggregate.setThree((ComponentThree) value);
              }
              return componentAggregate;
            }, Materialized.as(SCHOOL_AGGREGATE_STORE));


    windowSuppressTechnique(aggregate).through(AGGREGATE_UPDATES_TOPIC_SUPPRESS_UNTIL);

    windowSuppressTechniqueResetting(aggregate).through(AGGREGATE_UPDATES_TOPIC_SUPPRESS_CLOSES);

    customProcessorSuppressTechnique(builder, aggregate).through(AGGREGATE_UPDATES_TOPIC_CUSTOM);

    // deduplicator()
  }

  /**
   * Simple but the timer doesn't reset when a new aggregate is received
   *
   * @param aggregateStream
   * @return
   */
  private KStream<DocumentId, ComponentAggregate> windowSuppressTechnique(KTable<DocumentId, ComponentAggregate> aggregateStream) {
    Suppressed.StrictBufferConfig unbounded = unbounded();
    val suppress = Suppressed.untilTimeLimit(suppressionWindowTime, unbounded);
    KTable<DocumentId, ComponentAggregate> stream = aggregateStream.suppress(suppress);
    return stream.toStream();
  }

  /**
   * Complex, but time resets upon new aggregates, however runs on stream time and so needs arrival of new records to trigger.
   *
   * @param aggregateStream
   * @return
   */
  private KStream<DocumentId, ComponentAggregate> windowSuppressTechniqueResetting(KTable<DocumentId, ComponentAggregate> aggregateStream) {
    val suppressionWindow = TimeWindows.of(suppressionWindowTime);
    Suppressed<Windowed> suppression = Suppressed.untilWindowCloses(unbounded());

    SpecificAvroSerde<DocumentId> keySerde = ksUtils.serdeFor();
    SpecificAvroSerde<ComponentAggregate> valueSerde = ksUtils.serdeFor();

    val windowStream = aggregateStream
            .toStream()//Named.as("stream-for-suppression")) // TODO 2.4
            .groupByKey()
            .windowedBy(suppressionWindow)
//            .reduce((value1, value2) -> value2, Named.as("replace-with-new"), Materialized.with(keySerde, valueSerde)) // KAFKA-9259 // TODO 2.4
            .reduce((value1, value2) -> value2, Materialized.with(keySerde, valueSerde)) // KAFKA-9259
            .suppress(suppression);

    return windowStream
            //.toStream(Named.as("unsuppressed-updates-for-lookup")) // TODO 2.4
            .toStream()
            .map((windowedKey, suppressedWindowedValue) -> {
              DocumentId key = windowedKey.key();
              ReadOnlyKeyValueStore<DocumentId, ComponentAggregate> aggregateStore = getAggregateStore();
              ComponentAggregate schoolAggregateValueAndTimestamp = aggregateStore.get(key);
              val pair = KeyValue.pair(key, schoolAggregateValueAndTimestamp);
              return pair;
            //}, Named.as("lookup-full-aggregate-upon-trigger")); // TODO 2.4
            });
  }

  private ReadOnlyKeyValueStore<DocumentId, ComponentAggregate> getAggregateStore() {
    return this.storeProvider.getStore(SCHOOL_AGGREGATE_STORE, QueryableStoreTypes.<DocumentId, ComponentAggregate>keyValueStore());
  }

  /**
   * Complex but runs on wall clock time.
   *
   * @param builder
   * @param aggregateStream
   * @return
   */
  private KStream<DocumentId, ComponentAggregate> customProcessorSuppressTechnique(StreamsBuilder builder, KTable<DocumentId, ComponentAggregate> aggregateStream) {
    StoreBuilder<KeyValueStore<Long, ComponentAggregate>> suppressionStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SCHOOL_AGGREGATE_SUPPRESSION_STORE), Serdes.Long(), ksUtils.<ComponentAggregate>serdeFor()).withLoggingEnabled(Maps.newHashMap());
    builder.addStateStore(suppressionStoreBuilder);
    aggregateStream.toStream().process(()->new Processor<DocumentId, ComponentAggregate>() {
      @Override
      public void init(ProcessorContext context) {

      }

      @Override
      public void process(DocumentId key, ComponentAggregate value) {

      }

      @Override
      public void close() {

      }
    });
    return aggregateStream.toStream().flatTransformValues(() -> {
      return new ValueTransformerWithKey<DocumentId, ComponentAggregate, Iterable<ComponentAggregate>>() {

        private Duration SUPPRESSION_TIME = suppressionWindowTime;
        private KeyValueStore<Long, DocumentId> suppressionStore;
        private Duration SUPPRESSION_CHECK_FREQUENCY = Duration.ofMillis(100);

        @Override
        public void init(ProcessorContext context) {
          this.suppressionStore = (KeyValueStore<Long, DocumentId>) context.getStateStore(SCHOOL_AGGREGATE_SUPPRESSION_STORE);

          context.schedule(SUPPRESSION_CHECK_FREQUENCY, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long expireTime = System.currentTimeMillis() - SUPPRESSION_TIME.toMillis();
            KeyValueIterator<Long, DocumentId> expiredEntries = suppressionStore.range(0l, expireTime); // ready for emission
            expiredEntries.forEachRemaining(entry -> {
              DocumentId key = entry.value;
              ReadOnlyKeyValueStore<DocumentId, ComponentAggregate> aggregateStore = getAggregateStore();
              ComponentAggregate schoolAggregate = aggregateStore.get(key);
              context.forward(key, schoolAggregate);
//              context.
              suppressionStore.delete(expireTime);
            });
          });
        }

        @Override
        public Iterable<ComponentAggregate> transform(DocumentId key, ComponentAggregate value) {
          // update the last seen time stamp for suppression
          long now = System.currentTimeMillis();
          // TODO fix bug where if key arrives at different times, we get multiple entries instead of a single one
          // TODO fix bug where if multiple keys arrive at the same time stamp
          suppressionStore.put(now, key);
          return Lists.newArrayList(); // don't emit here, see punctuator
        }

        @Override
        public void close() {
          suppressionStore.close();
        }
      };
    }, SCHOOL_AGGREGATE_SUPPRESSION_STORE);
  }

}
