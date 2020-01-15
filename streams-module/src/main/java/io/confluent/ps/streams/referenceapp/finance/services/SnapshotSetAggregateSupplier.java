package io.confluent.ps.streams.referenceapp.finance.services;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;

import java.time.Instant;
import java.util.Optional;

public interface SnapshotSetAggregateSupplier {
  public Optional<SnapshotSetAggregation> findSnapSetAtWindowEndTime(SnapshotSetId snapshotSetId, Instant snapTime);
}
