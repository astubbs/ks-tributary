package io.confluent.ps.streams.referenceapp.finance.channels;

import com.google.common.collect.Lists;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;


/**
 * Mock channel
 */
@Slf4j
@Getter
@ToString
@AllArgsConstructor
@Builder(toBuilder = true)
public class MockChannels {

  @Nonnull
  private String channelId;

  /**
   * Just used to describe the set ("or category") the channel belongs to, just useful for logging distinctions
   */
  @Nonnull
  private String channelSetName;

  @Default
  private Boolean isNew = true;

  @Default
  private Optional<Instant> snapEndTimeSubscribedTo = Optional.empty();

  @Default
  private List<Object> channelOutputBuffer = Lists.newArrayList();

  // TODO remove and use lombok with Builder instead
  public MockChannels(String s, String sharded) {
    this.channelId = s;
    this.channelSetName = sharded;
    channelOutputBuffer = Lists.newArrayList();
    snapEndTimeSubscribedTo = Optional.empty();
    isNew = true;
  }

  public void send(SnapshotSetAggregation lastKnown) {
    markAsOld();
    channelOutputBuffer.add(lastKnown);
    log.debug("C:{} Sent aggregate: {}", channelId, lastKnown);
  }

  public void send(KeyValue kv) {
    markAsOld();
    channelOutputBuffer.add(kv);
    log.debug("C:{} Sent single: {}", channelId, kv);
  }

  private void markAsOld() {
    isNew = false;
  }

  public void send(InstrumentTick tick) {
    channelOutputBuffer.add(tick);
    log.debug("C:{} Sent single tick: {}", channelId, tick);
  }

}
