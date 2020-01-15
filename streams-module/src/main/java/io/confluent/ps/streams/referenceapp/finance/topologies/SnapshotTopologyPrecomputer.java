package io.confluent.ps.streams.referenceapp.finance.topologies;

import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.*;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.ChannelService;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannels;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetAggregateSupplier;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsConfigService;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Precompute the entire snapset in advance, for each window. This is so that upon query, we can serve the entire set for a snap time in one operation, instead of querying the latest instrument value once per instrument per snap set
 */
public abstract class SnapshotTopologyPrecomputer extends SnapshotTopologyParent {

  @Inject
  SnapshotSetsConfigService snapsetConfigService;

  private ChannelService channelService;

  private SnapshotSetAggregateSupplier aggregateSupplier;

  protected void inject(SnapshotSetAggregateSupplier aggregateSupplier, ChannelService cs) {
    this.aggregateSupplier = aggregateSupplier;
    this.channelService = cs;
  }

  @Inject
  void buildPrecomputedSnapsetTopology(StreamsBuilder builder, KStream<InstrumentId, InstrumentTickBD> instrumentStream) {
    // create a message for each instrument for each snapset config it matches (this occurs in memory only, used to trigger calculations)
    KStream<SnapshotSetId, InstrumentTickBD> fanOut = instrumentStream.flatMap(this::fanOutToSnapSets);

    process(builder, fanOut);
  }

  protected abstract void process(StreamsBuilder builder, KStream<SnapshotSetId, InstrumentTickBD> fanOut);

  protected Iterable<? extends KeyValue<? extends SnapshotSetId, ? extends InstrumentTickBD>> fanOutToSnapSets(InstrumentId instrumentId, InstrumentTickBD instrumentTick) {
    // create new message for each matching snap set
    Optional<InstrumentsToSnapshotSets> allMatchingSnapsetsForKey = snapsetConfigService.findAllMatchingSnapsetsForKeyFastInverseTechnique(instrumentId);
    List<KeyValue<SnapshotSetId, InstrumentTickBD>> fanouts = Lists.newArrayList();
    allMatchingSnapsetsForKey.ifPresent(it -> {
      it.getSnapSets().forEach(snapSetId -> {
        KeyValue<SnapshotSetId, InstrumentTickBD> snapSetTick = KeyValue.pair(snapSetId, instrumentTick);
        fanouts.add(snapSetTick);
      });
    });
    return fanouts;
  }

  protected void sendToChannelSubscribers(SnapshotSetId snapshotSetId, InstrumentTick tick) {
    List<MockChannels> channelSubscriptions = channelService.findChannelSubscriptions(snapshotSetId);
    channelSubscriptions.forEach(channel -> {

      // find out which window they're subscribed to
      Optional<Instant> snapEndTime = channel.getSnapEndTimeSubscribedTo();
      Instant now = getNow();
      Instant windowEndTime = snapEndTime.orElse(now); // default to now

      // bootstrap
      if (channel.getIsNew()) {
        // send bootstrap first, of all the keys that this shard hosts, from up stream ON SHARD

        Optional<SnapshotSetAggregation> snapSetAggregation = aggregateSupplier.findSnapSetAtWindowEndTime(snapshotSetId, windowEndTime);

        snapSetAggregation.ifPresent(channel::send);
      }

      // now forward the actual triggering instrument
      // send the live event if the window hasn't closed, or if it's live
      boolean afterSubscriptionTime = now.isBefore(windowEndTime);
      if (snapEndTime.isEmpty() || afterSubscriptionTime)
        channel.send(tick);
    });
  }


}
