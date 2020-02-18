package io.confluent.ps.streams.referenceapp.schools.topology;

import io.confluent.ps.streams.referenceapp.schools.model.SchoolEvent;
import io.confluent.ps.streams.referenceapp.schools.model.SchoolId;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.time.Duration;
import java.time.MonthDay;
import java.util.TimeZone;
import java.util.UUID;

import static java.time.Duration.ofMinutes;

public class SchoolTopology {

  private final KSUtils ksutils = new KSUtils();

  @Inject
  public SchoolTopology(StreamsBuilder builder) {
    KStream<SchoolId, SchoolEvent> stream = builder.stream("school-event");
  }

}
