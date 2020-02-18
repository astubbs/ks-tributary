package io.confluent.ps.streams.referenceapp.schools.topology;

import io.confluent.ps.streams.referenceapp.schools.model.OrgUnit;
import io.confluent.ps.streams.referenceapp.schools.model.SchStatusCode;
import io.confluent.ps.streams.referenceapp.schools.model.SchSubtype;
import io.confluent.ps.streams.referenceapp.schools.model.SchoolId;
import lombok.Data;
import lombok.val;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.inject.Inject;
import java.time.Duration;

public class SchoolTopology {

  public static final String ORG_TOPIC = "org_unit";
  public static final String SUP_TYPE_TOPIC = "sub_type";
  public static final String STATUS_CODE_TOPIC = "status_code";
  public static final String AGGREGATE_UPDATES_TOPIC = "aggregate_updates";

  @Inject
  public SchoolTopology(StreamsBuilder builder) {
    KStream<SchoolId, Object> orgTopicStream = builder.stream(ORG_TOPIC).selectKey((k, v) -> ((OrgUnit) v).getSchoolCode());
    KStream<SchoolId, Object> subTypeStream = builder.stream(SUP_TYPE_TOPIC).selectKey((k, v) -> ((SchSubtype) v).getSchoolCode());
    KStream<SchoolId, Object> statusCodeStream = builder.stream(STATUS_CODE_TOPIC).selectKey((k, v) -> ((SchStatusCode) v).getSchoolCode());

    KStream<SchoolId, Object> mergedStream = orgTopicStream.merge(subTypeStream).merge(statusCodeStream);

    val aggregateStream = mergedStream.groupByKey().aggregate(SchoolAggregate::new, (key, value, schoolAggregate) -> {
      if (value instanceof OrgUnit) {
        schoolAggregate.setOrg((OrgUnit) value);
      } else if (value instanceof SchSubtype) {
        schoolAggregate.setType((SchSubtype) value);
      } else if (value instanceof SchStatusCode) {
        schoolAggregate.setStatus((SchStatusCode) value);
      }
      return schoolAggregate;
    });
//
//    ValueTransformerWithKeySupplier<SchoolId, Object, Object> valueTransformerWithKeySupplier = new ValueTransformerWithKeySupplier<>() {
//      @Override
//      public ValueTransformerWithKey<SchoolId, Object, Object> get() {
//        return null;
//      }
//    };

    aggregateStream.toStream().transformValues(() -> {
      return new ValueTransformerWithKey<SchoolId, SchoolAggregate, SchoolAggregate>() {

        private Duration SUPPRESSION_TIME = Duration.ofSeconds(10);
        private KeyValueStore<SchoolId, SchoolAggregate> aggregateStore;
        private KeyValueStore<Long, SchoolId> suppressionStore;
        private int expectedRecordsCount = 3;

        @Override
        public void init(ProcessorContext context) {
          this.suppressionStore = (KeyValueStore<Long, SchoolId>) context.getStateStore("supressionStore");
          this.aggregateStore = (KeyValueStore<SchoolId, SchoolAggregate>) context.getStateStore("aggregateStore");

          context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long expireTime = System.currentTimeMillis() - SUPPRESSION_TIME.toMillis();
            KeyValueIterator<Long, SchoolId> expiredEntries = suppressionStore.range(0l, expireTime); // ready for emission
            expiredEntries.forEachRemaining(entry -> {
              SchoolId key = entry.value;
              SchoolAggregate schoolAggregate = aggregateStore.get(key);
              context.forward(key, schoolAggregate);
            });
          });
        }

        @Override
        public SchoolAggregate transform(SchoolId key, SchoolAggregate value) {
          aggregateStore.put(key, value);
          return null; // don't emit here, see punctuator
        }

        @Override
        public void close() {
          aggregateStore.close();
          suppressionStore.close();
        }
      };
    }, "aggregate-dedupper");
  }

  @Data
  class SchoolAggregate {
    private OrgUnit org;
    private SchSubtype type;
    private SchStatusCode status;
  }

}
