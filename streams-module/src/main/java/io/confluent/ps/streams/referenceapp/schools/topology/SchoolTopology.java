package io.confluent.ps.streams.referenceapp.schools.topology;

import io.confluent.ps.streams.referenceapp.schools.model.OrgUnit;
import io.confluent.ps.streams.referenceapp.schools.model.SchStatusCode;
import io.confluent.ps.streams.referenceapp.schools.model.SchSubtype;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import lombok.Data;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import javax.inject.Inject;

public class SchoolTopology {

  public static final String SCHOOL_EVENT_TOPIC = "org_unit";
  private final KSUtils ksutils = new KSUtils();

  @Inject
  public SchoolTopology(StreamsBuilder builder) {
    KStream<Object, Object> orgTopicStream = builder.stream(SCHOOL_EVENT_TOPIC).selectKey((k, v) -> ((OrgUnit) v).getSchoolCode());
    KStream<Object, Object> subTypeStream = builder.stream(SCHOOL_EVENT_TOPIC).selectKey((k, v) -> ((SchSubtype) v).getSchoolCode());
    KStream<Object, Object> statusCodeStream = builder.stream(SCHOOL_EVENT_TOPIC).selectKey((k, v) -> ((SchStatusCode) v).getSchoolCode());

    KStream<Object, Object> mergedStream = orgTopicStream.merge(subTypeStream).merge(statusCodeStream);

    mergedStream.groupByKey().aggregate(Aggregate::new, (key, value, aggregate) -> {
      if (value instanceof OrgUnit) {
        aggregate.setOrg((OrgUnit) value);
      } else if (value instanceof SchSubtype) {
        aggregate.setType((SchSubtype) value);
      } else if (value instanceof SchStatusCode) {
        aggregate.setStatus((SchStatusCode) value);
      }
      return aggregate;
    });
  }

  @Data
  class Aggregate {
    private OrgUnit org;
    private SchSubtype type;
    private SchStatusCode status;
  }

}
