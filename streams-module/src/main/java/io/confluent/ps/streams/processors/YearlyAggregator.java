package io.confluent.ps.streams.referenceapp.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Instant;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.ZonedDateTime;
import java.util.TimeZone;

/**
 * @param <K>  key type
 * @param <V>  value type
 * @param <AV> aggregator type
 */
public class YearlyAggregator<K, V, AV> implements ValueTransformerWithKey<K, V, KeyValue<K, AV>> {

  private final Initializer<AV> initializer;
  private final String storeName;

  private WindowStore<K, AV> stateStore;

  private final MonthDay startingMonthDay;
  private final TimeZone timezone;
  private final Aggregator<K, V, AV> aggregator;
  private ProcessorContext context;

  /**
   * @param startingMonthDay
   * @param timezone         time zone of the date to be used
   * @param initializer
   * @param aggregator       (must handle nulls)
   * @param storeName        name of the store to use
   */
  public YearlyAggregator(MonthDay startingMonthDay, TimeZone timezone, final Initializer<AV> initializer, Aggregator<K, V, AV> aggregator, String storeName) {
    this.startingMonthDay = startingMonthDay;
    this.timezone = timezone;
    this.aggregator = aggregator;
    this.initializer = initializer;
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.stateStore = (WindowStore<K, AV>) context.getStateStore(storeName);
  }

  @Override
  public KeyValue<K, AV> transform(K key, V newValue) {
    long messageTime = context.timestamp();
    long yearWindowStart = getWindowStartForMessageEpoch(messageTime);

    // check for old value
    AV oldValue = stateStore.fetch(key, yearWindowStart);
    if (oldValue != null) {
      oldValue = initializer.apply();
    }

    // apply the aggregation function
    AV aggValue = aggregator.apply(key, newValue, oldValue);

    // record the new aggregated value
    stateStore.put(key, aggValue, yearWindowStart);

    // emit the update in case it's useful
    return KeyValue.pair(key, aggValue);
  }

  @Override
  public void close() {
    this.stateStore.close();
  }

  /**
   * Given a message epoch ms timestamp, find the corresponding year (with date offset) bucket start time it falls into
   * @param messageTime
   * @return
   */
  public ZonedDateTime getWindowStartForMessage(long messageTime) {
    // get message zoned timestamp
    Instant messageInstant = Instant.ofEpochMilli(messageTime);
    ZonedDateTime zonedMessageTime = messageInstant.atZone(timezone.toZoneId());

    // get start date in message year
    LocalDate startDateInMessageYear = this.startingMonthDay.atYear(zonedMessageTime.getYear());
    ZonedDateTime zonedYearStart = startDateInMessageYear.atStartOfDay(timezone.toZoneId());

    // if message is after star date, use it, otherwise it must be last years
    boolean after = zonedMessageTime.isAfter(zonedYearStart);
    if (after) {
      // is within current start year
      return zonedYearStart;
    } else {
      // is before, use previous year start
      ZonedDateTime zonedMessageStartLastYear = startDateInMessageYear.minusYears(1).atStartOfDay(timezone.toZoneId());
      return zonedMessageStartLastYear;
    }
  }

  public long getWindowStartForMessageEpoch(long messageTime) {
    ZonedDateTime windowStartForMessage = this.getWindowStartForMessage(messageTime);
    return windowStartForMessage.toInstant().toEpochMilli();
  }
}
