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
import org.apache.kafka.streams.state.KeyValueStore;

import javax.inject.Inject;

public class SchoolTopology {

  public static final String ORG_TOPIC = "org_unit";
  public static final String SUP_TYPE_TOPIC = "sub_type";
  public static final String STATUS_CODE_TOPIC = "status_code";
  public static final String AGGREGATE_UPDATES_TOPIC = "aggregate_updates";

  @Inject
  public SchoolTopology(StreamsBuilder builder) {
    KStream<Object, Object> orgTopicStream = builder.stream(ORG_TOPIC).selectKey((k, v) -> ((OrgUnit) v).getSchoolCode());
    KStream<Object, Object> subTypeStream = builder.stream(SUP_TYPE_TOPIC).selectKey((k, v) -> ((SchSubtype) v).getSchoolCode());
    KStream<Object, Object> statusCodeStream = builder.stream(STATUS_CODE_TOPIC).selectKey((k, v) -> ((SchStatusCode) v).getSchoolCode());

    KStream<Object, Object> mergedStream = orgTopicStream.merge(subTypeStream).merge(statusCodeStream);

    val aggregateStream = mergedStream.groupByKey().aggregate(Aggregate::new, (key, value, aggregate) -> {
      if (value instanceof OrgUnit) {
        aggregate.setOrg((OrgUnit) value);
      } else if (value instanceof SchSubtype) {
        aggregate.setType((SchSubtype) value);
      } else if (value instanceof SchStatusCode) {
        aggregate.setStatus((SchStatusCode) value);
      }
      return aggregate;
    });

    aggregateStream.toStream().transformValues(() -> {
      return new ValueTransformerWithKey<SchoolId, Aggregate, Aggregate>() {

        private KeyValueStore<SchoolId, Integer> stateStore;
        private int expectedRecordsCount = 3;

        @Override
        public void init(ProcessorContext context) {
          this.stateStore = (KeyValueStore<SchoolId, Integer>) context.getStateStore("storeName");
        }

        @Override
        public Aggregate transform(SchoolId key, Aggregate value) {
          Integer integer = stateStore.get(key);
          if (integer == expectedRecordsCount) {
            // emit
            // reset
          } else {
            // update
            // ignore
          }
          return null;
        }

        @Override
        public void close() {

        }
      };
    }, "aggregate-dedupper");
  }

  @Data
  class Aggregate {
    private OrgUnit org;
    private SchSubtype type;
    private SchStatusCode status;
  }

}
