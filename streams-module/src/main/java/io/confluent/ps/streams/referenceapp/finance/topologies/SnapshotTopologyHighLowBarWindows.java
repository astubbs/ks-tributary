package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.HighLow;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMinutes;

/**
 * Precompute high low values for time frames, and make the data available for query.
 */
@Slf4j
public class SnapshotTopologyHighLowBarWindows extends SnapshotTopologyParent {

  public final static String HIGH_LOW_BARS_ONE_MINUTE_STORE_NAME = "high-low-bars-one-minute-store";
  public final static String HIGH_LOW_BARS_TEN_MINUTE_STORE_NAME = "high-low-bars-ten-minutes-store";

  final Duration oneMinute = ofMinutes(1);
  final Duration tenMinutes = ofMinutes(10);

  final Duration retentionPeriod = ofDays(3); // Retention must be longer than window time

  final TimeWindows oneMinuteWindow = TimeWindows.of(oneMinute).grace(oneMinute.dividedBy(2));
  final TimeWindows tenMinuteWindow = TimeWindows.of(tenMinutes).grace(oneMinute.dividedBy(2));

  @Inject
  public SnapshotTopologyHighLowBarWindows(KStream<InstrumentId, InstrumentTickBD> instrumentStream){
    this.latestPerInstrumentTopology(instrumentStream);
  }

  void latestPerInstrumentTopology(KStream<InstrumentId, InstrumentTickBD> instrumentStream) {
    TimeWindowedKStream<InstrumentId, InstrumentTickBD> oneMinuteStream = instrumentStream.groupByKey().windowedBy(oneMinuteWindow);
    TimeWindowedKStream<InstrumentId, InstrumentTickBD> tenMinuteStream = instrumentStream.groupByKey().windowedBy(tenMinuteWindow);

    appendHighLowCalculation(oneMinuteStream, HIGH_LOW_BARS_ONE_MINUTE_STORE_NAME);
    appendHighLowCalculation(tenMinuteStream, HIGH_LOW_BARS_TEN_MINUTE_STORE_NAME);
  }

  private void appendHighLowCalculation(TimeWindowedKStream<InstrumentId, InstrumentTickBD> xMinuteStream, String storeName) {
    xMinuteStream.aggregate(HighLow::new, (instrumentId, newTick, highLowAggregate) -> {
      // TODO extract this function and test
      InstrumentTickBD currentHigh = InstrumentTickBD.of(highLowAggregate.getHigh());

      if (newTick.isPriceGreaterThan(currentHigh)) {
        highLowAggregate.setHigh(newTick);
      } else {
        highLowAggregate.setHigh(currentHigh);
      }

      InstrumentTickBD low = InstrumentTickBD.of(highLowAggregate.getLow());

      if (newTick.isPriceLesserThan(low)) {
        highLowAggregate.setLow(newTick);
      } else {
        highLowAggregate.setLow(low);
      }

      return highLowAggregate;
    }, Materialized.as(storeName));
  }

  public Optional<HighLow> findHighLowOneMinute(Instant windowEndingAt, InstrumentId instrumentId) {
    WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> instrumentIdValueAndTimestampWindowStore = stores.highLowBarsOneMinuteStore();
    Instant start = windowEndingAt.minus(oneMinute);
    return getHighLow(instrumentId, start, instrumentIdValueAndTimestampWindowStore);
  }

  public Optional<HighLow> findHighLowTenMinute(Instant windowEndingAt, InstrumentId instrumentId) {
    WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> instrumentIdValueAndTimestampWindowStore = stores.highLowBarsTenMinuteStore();
    Instant start = windowEndingAt.minus(tenMinutes);
    return getHighLow(instrumentId, start, instrumentIdValueAndTimestampWindowStore);
  }

  private Optional<HighLow> getHighLow(InstrumentId instrumentOne, Instant windowStart,
                                       WindowStore<InstrumentId, ValueAndTimestamp<HighLow>> instrumentIdValueAndTimestampWindowStore) {
    ValueAndTimestamp<HighLow> fetch = instrumentIdValueAndTimestampWindowStore.fetch(instrumentOne, windowStart.toEpochMilli());
    if (fetch == null) return Optional.empty();
    else return Optional.of(fetch.value());
  }

}
