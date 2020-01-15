package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSharded;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsConfigService;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsPrecomputedShardedService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class SnapshotTopologyPrecomputerSharded extends SnapshotTopologyPrecomputer {

  public final static String PRECOMPUTED_SNAPSET_SHARDS = "precomputed-snapset-shards";

  @Inject
  SnapshotSetsConfigService snapsetConfigService;

  @Inject
  MockChannelServiceSharded csShardedClients;

  @Inject
  SnapshotSetsPrecomputedShardedService shardedService;

  @Inject
  public SnapshotTopologyPrecomputerSharded(SnapshotSetsConfigService snapsetConfigService, MockChannelServiceSharded csShardedClients, SnapshotSetsPrecomputedShardedService shardedService) {
    this.snapsetConfigService = snapsetConfigService;
    this.csShardedClients = csShardedClients;
    this.shardedService = shardedService;
  }

  /**
   * Normally would do this in a module in production, but this is an easy way to have two in parallel
   */
  @Inject
  void injectParent() {
    super.inject(shardedService, csShardedClients);
  }

  /**
   * This technique has the advantage that it's fully distributed. Minimise network hops. Distributes computing across all KS nodes.
   *
   * @param builder
   * @param snapSetIdKeyValueKStream
   */
  @Override
  protected void process(StreamsBuilder builder, KStream<SnapshotSetId, InstrumentTickBD> snapSetIdKeyValueKStream) {
    // setup store
    StoreBuilder earlyResponseStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(PRECOMPUTED_SNAPSET_SHARDS,
            settings.retentionPeriod,
            settings.windowSize,
            false
            ),
            ksutils.serdeFor(true), ksutils.serdeFor(false));

    builder.addStateStore(earlyResponseStoreBuilder);

    /**
     * Save latest instruments into respective windows. This technique saves the data into aggregated chunks, for all keys that arrive
     * on this instance. If there are many many instruments in the set, this may become slow, as the aggregate object becomes larger - but
     * the read path is a single operation as the aggregate has already been calculated.
     *
     * A compound key where you just insert new entries, and do a range scan on read path might be a happy balance
     */
    KStream<SnapshotSetId, InstrumentTick> recordedTickStream = snapSetIdKeyValueKStream.transform(() -> {
      return new Transformer<SnapshotSetId, InstrumentTickBD, KeyValue<SnapshotSetId, InstrumentTick>>() {
        WindowStore<SnapshotSetId, SnapshotSetAggregation> store;
        ProcessorContext context;

        public void init(ProcessorContext context) {
          this.context = context;
          this.store = (WindowStore<SnapshotSetId, SnapshotSetAggregation>) context.getStateStore(PRECOMPUTED_SNAPSET_SHARDS);
        }

        @Override
        public KeyValue<SnapshotSetId, InstrumentTick> transform(SnapshotSetId snapSetId, InstrumentTickBD snapSetTick) {
          return aggregateAndForward(snapSetId, snapSetTick, context, store);
        }

        public void close() {
          // state stores are closed by the framework
        }
      };
    }, PRECOMPUTED_SNAPSET_SHARDS);

    // after recording into windows, for each record, send the channel subscribers
    recordedTickStream.foreach(super::sendToChannelSubscribers);
  }

  private KeyValue<SnapshotSetId, InstrumentTick> aggregateAndForward(SnapshotSetId snapshotSetId,
                                                                      InstrumentTickBD snapSetTick,
                                                                      ProcessorContext context,
                                                                      WindowStore<SnapshotSetId, SnapshotSetAggregation> store) {
    Instant recordTimestamp = Instant.ofEpochMilli(context.timestamp());
    // calculate matching windows
    Map<Long, TimeWindow> windows = settings.timeWindow.windowsFor(recordTimestamp.toEpochMilli());
    // create an entry for each window / replace any existing
    windows.forEach((windowStartTime, window) -> {
      // fetch existing aggregate
      Instant windowStartInstant = window.startTime();
      Optional<SnapshotSetAggregation> snapSetAtTime = shardedService.findSnapSetAtWindowStartTime(snapshotSetId, windowStartInstant);

      // initialise
      SnapshotSetAggregation aggregateToUpdate = snapSetAtTime.orElseGet(() -> SnapshotSetAggregation.newBuilder().setId(snapshotSetId).build());

      String id = snapSetTick.getId().getId();
      String price = snapSetTick.getPrice();
      aggregateToUpdate.getInstruments().put(id, price);

      // update it
      store.put(snapshotSetId, aggregateToUpdate, windowStartTime);
    });

    // forward downstream
    return KeyValue.pair(snapshotSetId, snapSetTick);
  }

}
