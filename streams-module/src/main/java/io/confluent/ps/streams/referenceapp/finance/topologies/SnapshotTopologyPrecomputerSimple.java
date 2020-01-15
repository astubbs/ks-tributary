package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSimple;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsPrecomputedSimpleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;
import java.time.Duration;


@Slf4j
public class SnapshotTopologyPrecomputerSimple extends SnapshotTopologyPrecomputer {

  public final static String PRECOMPUTED_SNAPSET_SIMPLE = "precomputed-snapset-simple";

  @Inject
  MockChannelServiceSimple csSimpleClients;

  @Inject
  SnapshotSetsPrecomputedSimpleService snapSetsPrecomputedSimpleService;

  @Inject
  public SnapshotTopologyPrecomputerSimple(MockChannelServiceSimple csSimpleClients, SnapshotSetsPrecomputedSimpleService snapSetsPrecomputedSimpleService) {
    this.csSimpleClients = csSimpleClients;
    this.snapSetsPrecomputedSimpleService = snapSetsPrecomputedSimpleService;
  }

  /**
   * Normally would do this in a module in production, but this is an easy way to have two in parallel
   */
  @Inject
  void injectParent() {
    super.inject(snapSetsPrecomputedSimpleService, csSimpleClients);
  }

  /**
   * This technique is very simple and more clearly correct, however all snap subscriptions will be glued to one instance. This means that if there's a busy snap set, one of the KS instances will bear all the old.
   *
   * @param snapSetIdKeyValueKStream
   */
  @Override
  protected void process(StreamsBuilder builder, KStream<SnapshotSetId, InstrumentTickBD> snapSetIdKeyValueKStream) {

    // send to channels subscribed to specific set
    snapSetIdKeyValueKStream.foreach(this::sendToChannelSubscribers);

    // store
    Duration retentionPeriodOfAggregates = settings.windowSize.multipliedBy(3); //days

    // topology
    TimeWindowedKStream<SnapshotSetId, InstrumentTickBD> snapSetIdKeyValueTimeWindowedKStream = snapSetIdKeyValueKStream.groupByKey().windowedBy(settings.timeWindow);

    performAggregate(retentionPeriodOfAggregates, snapSetIdKeyValueTimeWindowedKStream);

  }

  private void performAggregate(Duration retentionPeriodOfAggregates, TimeWindowedKStream<SnapshotSetId, InstrumentTickBD> snapSetIdKeyValueTimeWindowedKStream) {
    snapSetIdKeyValueTimeWindowedKStream.aggregate(() -> {
      SnapshotSetId dummyId = SnapshotSetId.newBuilder().setId("").build(); // TODO model doesn't allow null
      return SnapshotSetAggregation.newBuilder().setId(dummyId).build();
    }, (snapSetId, snapSetTick, precomputedSnap) ->
    {
      precomputedSnap.setId(snapSetId); // TODO aggregator doesn't pass in the key, so we have to set it here
      // add or replace old entry with newer
      InstrumentId stringKeyConverted = snapSetTick.getId(); // have to use strings for avro map keys (see schema)
      precomputedSnap.getInstruments().put(stringKeyConverted.getId(), snapSetTick.getPrice());
      return precomputedSnap;
    }, Materialized.<SnapshotSetId, SnapshotSetAggregation, WindowStore<Bytes, byte[]>>as(PRECOMPUTED_SNAPSET_SIMPLE).withRetention(retentionPeriodOfAggregates));
  }

}
