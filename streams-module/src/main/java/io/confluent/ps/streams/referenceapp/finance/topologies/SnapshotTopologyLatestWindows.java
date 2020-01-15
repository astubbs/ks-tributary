package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.services.LatestInstrumentWindowsService;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceIndividual;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannels;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

/**
 * Tracks latest values per window, sends updates downstream. Cannot boot strap because there's no sense of subscribing to a collection of instruments, only individual instruments, so there's no collected set to bootstrap onto a channel upon first subscription.
 * <p>
 * Provides a queryable window store of each individual keys last seen values in window.
 */
@Slf4j
public class SnapshotTopologyLatestWindows extends SnapshotTopologyParent {

  public final static String LATEST_INSTRUMENT_WINDOWS_STORE_NAME = "latest-seen-instrument";

  @Inject
  SnapshotSetsConfigService configService;

  @Inject
  MockChannelServiceIndividual csIndividualClients;

  @Inject
  LatestInstrumentWindowsService iss;

  @Inject
  public SnapshotTopologyLatestWindows(SnapshotSetsConfigService configService, MockChannelServiceIndividual csIndividualClients, LatestInstrumentWindowsService iss) {
    this.configService = configService;
    this.csIndividualClients = csIndividualClients;
    this.iss = iss;
  }

  /**
   * Simplest of the three topologies - computes the latest instrument value per window, and send the stream to users who are subscribed via channels. If the number of instruments individually subscribed to is large, then querying the state store for all the initial values could be slow.
   *
   * @param instrumentStream
   * @return
   */
  @Inject
  void latestPerInstrumentTopology(KStream<InstrumentId, InstrumentTickBD> instrumentStream) {
    // send to channels subscribed to specific key
    instrumentStream.foreach((instrumentId, tick) ->
    {
      // find channels subscribed to this specific key
      List<MockChannels> allMatchingSnapsetsForKey = csIndividualClients.findExplicitChannelSubscriptionsForKey(instrumentId);
      allMatchingSnapsetsForKey.forEach(channel -> {
        // find out which window they're subscribed to
        Optional<Instant> snapEndTime = channel.getSnapEndTimeSubscribedTo();
        Instant now = getNow();
        Instant windowEndTime = snapEndTime.orElse(now); // default to now

        // send the live event if the window hasn't closed, or if it's live
        if (snapEndTime.isEmpty() || now.isBefore(windowEndTime))
          channel.send(KeyValue.pair(instrumentId, tick));
      });
    });

    KGroupedStream<InstrumentId, InstrumentTickBD> stringIntegerKGroupedStream = instrumentStream.groupByKey();
    TimeWindowedKStream<InstrumentId, InstrumentTickBD> windowsInstrumentStream = stringIntegerKGroupedStream.windowedBy(settings.timeWindow);

    // find max instrument value
    Materialized<InstrumentId, InstrumentTickBD, WindowStore<Bytes, byte[]>> materialized = Materialized.<InstrumentId, InstrumentTickBD, WindowStore<Bytes, byte[]>>as(LATEST_INSTRUMENT_WINDOWS_STORE_NAME).withRetention(settings.retentionPeriod);

    KTable<Windowed<InstrumentId>, InstrumentTickBD> reduced = windowsInstrumentStream.reduce
            ((a, n) -> n, // replace with newer
                    materialized);

    // emit final window values
    reduced.suppress(Suppressed.untilWindowCloses(unbounded()))
            .toStream()
            .to(FINAL_SNAP_VALUES_TOPIC_NAME);
  }

}
