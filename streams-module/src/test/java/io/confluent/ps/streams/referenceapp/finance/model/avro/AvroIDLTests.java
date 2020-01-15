package io.confluent.ps.streams.referenceapp.finance.model.avro;

import com.google.common.collect.Maps;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroIDLTests {

  @Test
  void testBuildIdlComplexObjects() {
    SnapshotSetId id = SnapshotSetId.newBuilder().setId("0").build();
    SnapshotSetAggregation snapshotSetAggregation = SnapshotSetAggregation.newBuilder().setId(id).setInstruments(Maps.newHashMap()).build();
    assertThat(snapshotSetAggregation.getInstruments()).isNotNull();
  }

  @Test
  void testDefaultValues() {
    SnapshotSetId id = SnapshotSetId.newBuilder().setId("0").build();
    // should come with default empty map
    SnapshotSetAggregation snapshotSetAggregation = SnapshotSetAggregation.newBuilder().setId(id).build();
    assertThat(snapshotSetAggregation.getInstruments()).isNotNull();
  }
}
