package io.confluent.ps.streams.referenceapp.finance.model.avro;

import com.google.common.collect.Maps;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroTests {

  @Test
  void testBuildComplexObjects() {
    SnapshotSetId id = SnapshotSetId.newBuilder().setId("0").build();
    SnapshotSetAggregation snapshotSetAggregation = SnapshotSetAggregation.newBuilder().setId(id).setInstruments(Maps.newHashMap()).build();
    assertThat(snapshotSetAggregation.getInstruments()).isNotNull();
  }

}
