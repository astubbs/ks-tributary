package io.confluent.ps.streams.referenceapp.schools.topology;

import com.google.common.collect.Maps;
import io.confluent.ps.streams.referenceapp.schools.model.*;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import javax.inject.Inject;
import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class SchoolTopology {

  final KSUtils ksUtils;

  public static final String ORG_UNIT_TOPIC = "org_unit";
  public static final String SUP_TYPE_TOPIC = "sub_type";
  public static final String STATUS_CODE_TOPIC = "status_code";
  public static final String AGGREGATE_UPDATES_TOPIC = "aggregate_updates";
  public static final String SCHOOL_AGGREGATE_STORE = "school-aggregate-store";
  public static final String SCHOOL_AGGREGATE_SUPPRESSION_STORE = "school-aggregate-suppression-store";

  private final Duration suppressionWindowTime = Duration.ofSeconds(20);

  private final StoreProvider storeProvider;

  @Inject
  public SchoolTopology(StreamsBuilder builder, StoreProvider storeProvider, KSUtils ksUtils) {
    this.storeProvider = storeProvider;
    this.ksUtils = ksUtils;

    buildTopology(builder);
  }

  private void buildTopology(StreamsBuilder builder) {
    KStream<SchoolId, Object> orgTopicStream = builder.stream(ORG_UNIT_TOPIC)
            .selectKey((k, v) -> ((OrgUnit) v).getSchoolCode());
    KStream<SchoolId, Object> subTypeStream = builder.stream(SUP_TYPE_TOPIC)
            .selectKey((k, v) -> ((SchSubtype) v).getSchoolCode());
    KStream<SchoolId, Object> statusCodeStream = builder.stream(STATUS_CODE_TOPIC)
            .selectKey((k, v) -> ((SchStatusCode) v).getSchoolCode());

    KStream<SchoolId, Object> mergedStream = orgTopicStream
            .merge(subTypeStream)
            .merge(statusCodeStream);

    KTable<SchoolId, SchoolAggregate> aggregate = mergedStream
            .groupByKey()
            .aggregate(SchoolAggregate::new, (key, value, schoolAggregate) -> {

              schoolAggregate.setId(key); // yuck

              if (value instanceof OrgUnit) {
                schoolAggregate.setOrg((OrgUnit) value);
              } else if (value instanceof SchSubtype) {
                schoolAggregate.setType((SchSubtype) value);
              } else if (value instanceof SchStatusCode) {
                schoolAggregate.setStatusCode((SchStatusCode) value);
              }
              return schoolAggregate;
            }, Materialized.as(SCHOOL_AGGREGATE_STORE));


    windowSuppressTechnique(aggregate);

//    windowSuppressTechniqueResetting(aggregate);
//
//    customProcessorSuppressTechnique(builder, aggregate);
  }

  /**
   * Simple but the timer doesn't reset when a new aggregate is received
   * @param aggregateStream
   * @return
   */
  private KStream<SchoolId, SchoolAggregate> windowSuppressTechnique(KTable<SchoolId, SchoolAggregate> aggregateStream) {
    val suppress = Suppressed.untilTimeLimit(suppressionWindowTime, unbounded());
    KTable<SchoolId, SchoolAggregate> stream = aggregateStream.suppress(suppress);
    return stream.toStream();
  }

  /**
   * Complex, but time resets upon new aggregates, however runs on stream time and so needs arrival of new records to trigger.
   *
   * @param aggregateStream
   * @return
   */
  private KStream<SchoolId, SchoolAggregate> windowSuppressTechniqueResetting(KTable<SchoolId, SchoolAggregate> aggregateStream) {
    val suppressionWindow = TimeWindows.of(suppressionWindowTime);
    Suppressed<Windowed> suppression = Suppressed.untilWindowCloses(unbounded());

    val windowStream = aggregateStream
            .toStream()
            .groupByKey()
            .windowedBy(suppressionWindow)
            .reduce((value1, value2) -> value2)
            .suppress(suppression);

    return windowStream
            .toStream()
            .map((windowedKey, suppressedWindowedValue) -> {
              SchoolId key = windowedKey.key();
              ReadOnlyKeyValueStore<SchoolId, SchoolAggregate> aggregateStore = getAggregateStore();
              SchoolAggregate schoolAggregateValueAndTimestamp = aggregateStore.get(key);
              val pair = KeyValue.pair(key, schoolAggregateValueAndTimestamp);
              return pair;
            });
  }

  private ReadOnlyKeyValueStore<SchoolId, SchoolAggregate> getAggregateStore() {
    return this.storeProvider.getStore(SCHOOL_AGGREGATE_STORE, QueryableStoreTypes.<SchoolId, SchoolAggregate>keyValueStore());
  }

  /**
   * Complex but runs on wall clock time.
   *
   *
   * @param builder
   * @param aggregateStream
   * @return
   */
  private KStream<SchoolId, SchoolAggregate> customProcessorSuppressTechnique(StreamsBuilder builder, KTable<SchoolId, SchoolAggregate> aggregateStream) {
    StoreBuilder<KeyValueStore<Long, SchoolAggregate>> suppressionStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SCHOOL_AGGREGATE_SUPPRESSION_STORE), Serdes.Long(), ksUtils.<SchoolAggregate>serdeFor()).withLoggingEnabled(Maps.newHashMap());
    builder.addStateStore(suppressionStoreBuilder);

    return aggregateStream.toStream().transformValues(() -> {
      return new ValueTransformerWithKey<SchoolId, SchoolAggregate, SchoolAggregate>() {

        private Duration SUPPRESSION_TIME = Duration.ofSeconds(10);
        private KeyValueStore<Long, SchoolId> suppressionStore;
        private Duration SUPPRESSION_CHECK_FREQUENCY = Duration.ofSeconds(15);

        @Override
        public void init(ProcessorContext context) {
          this.suppressionStore = (KeyValueStore<Long, SchoolId>) context.getStateStore(SCHOOL_AGGREGATE_SUPPRESSION_STORE);

          context.schedule(SUPPRESSION_CHECK_FREQUENCY, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long expireTime = System.currentTimeMillis() - SUPPRESSION_TIME.toMillis();
            KeyValueIterator<Long, SchoolId> expiredEntries = suppressionStore.range(0l, expireTime); // ready for emission
            expiredEntries.forEachRemaining(entry -> {
              SchoolId key = entry.value;
              ReadOnlyKeyValueStore<SchoolId, SchoolAggregate> aggregateStore = getAggregateStore();
              SchoolAggregate schoolAggregate = aggregateStore.get(key);
              context.forward(key, schoolAggregate);
              suppressionStore.delete(expireTime);
            });
          });
        }

        @Override
        public SchoolAggregate transform(SchoolId key, SchoolAggregate value) {
          // update the last seen time stamp for suppression
          long now = System.currentTimeMillis();
          suppressionStore.put(now, key);
          return null; // don't emit here, see punctuator
        }

        @Override
        public void close() {
          suppressionStore.close();
        }
      };
    }, SCHOOL_AGGREGATE_SUPPRESSION_STORE).through(AGGREGATE_UPDATES_TOPIC);
  }

}
