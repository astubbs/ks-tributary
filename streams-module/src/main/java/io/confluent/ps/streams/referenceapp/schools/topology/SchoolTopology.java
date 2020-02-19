package io.confluent.ps.streams.referenceapp.schools.topology;

import io.confluent.ps.streams.referenceapp.schools.model.*;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
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

  public static final String ORG_UNIT_TOPIC = "org_unit";
  public static final String SUP_TYPE_TOPIC = "sub_type";
  public static final String STATUS_CODE_TOPIC = "status_code";
  public static final String AGGREGATE_UPDATES_TOPIC = "aggregate_updates";
  public static final String SCHOOL_AGGREGATE_STORE = "school-aggregate-store";

  private final Duration suppressionWindowTime = Duration.ofSeconds(20);

  @Inject
  public SchoolTopology(StreamsBuilder builder, StoreProvider storeProvider) {
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

    windowSuppressTechniqueRessetting(aggregate, storeProvider);

    customProcessorSuppressTechnique(aggregate);
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
   * @param storeProvider
   * @return
   */
  private KStream<SchoolId, SchoolAggregate> windowSuppressTechniqueRessetting(KTable<SchoolId, SchoolAggregate> aggregateStream,
                                                                               StoreProvider storeProvider) {
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
      ReadOnlyKeyValueStore<SchoolId, SchoolAggregate> aggregateStore = storeProvider.getStore(SCHOOL_AGGREGATE_STORE, QueryableStoreTypes.keyValueStore());
      SchoolAggregate schoolAggregateValueAndTimestamp = aggregateStore.get(key);
      val pair = KeyValue.pair(key, schoolAggregateValueAndTimestamp);
      return pair;
    });
  }

  /**
   * Complex but runs on wall clock time.
   *
   * @param aggregateStream
   * @return
   */
  private KStream<SchoolId, SchoolAggregate> customProcessorSuppressTechnique(KTable<SchoolId, SchoolAggregate> aggregateStream) {
    return aggregateStream.toStream().transformValues(() -> {
      return new ValueTransformerWithKey<SchoolId, SchoolAggregate, SchoolAggregate>() {

        private Duration SUPPRESSION_TIME = Duration.ofSeconds(10);
        private KeyValueStore<SchoolId, SchoolAggregate> aggregateStore;
        private KeyValueStore<Long, SchoolId> suppressionStore;
        private Duration SUPPRESSION_CHECK_FREQUENCY = Duration.ofSeconds(15);

        @Override
        public void init(ProcessorContext context) {
          this.suppressionStore = (KeyValueStore<Long, SchoolId>) context.getStateStore("supressionStore");
          this.aggregateStore = (KeyValueStore<SchoolId, SchoolAggregate>) context.getStateStore("aggregateStore");


          context.schedule(SUPPRESSION_CHECK_FREQUENCY, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long expireTime = System.currentTimeMillis() - SUPPRESSION_TIME.toMillis();
            KeyValueIterator<Long, SchoolId> expiredEntries = suppressionStore.range(0l, expireTime); // ready for emission
            expiredEntries.forEachRemaining(entry -> {
              SchoolId key = entry.value;
              SchoolAggregate schoolAggregate = aggregateStore.get(key);
              context.forward(key, schoolAggregate);
              suppressionStore.delete(expireTime);
              aggregateStore.delete(key);
            });
          });
        }

        @Override
        public SchoolAggregate transform(SchoolId key, SchoolAggregate value) {
          aggregateStore.put(key, value); // store the aggregation
          // update the last seen time stamp for suppression
          long now = System.currentTimeMillis();
          suppressionStore.put(now, key);
          return null; // don't emit here, see punctuator
        }

        @Override
        public void close() {
          aggregateStore.close();
          suppressionStore.close();
        }
      };
    }, "aggregate-dedupper").through(AGGREGATE_UPDATES_TOPIC);
  }

}
