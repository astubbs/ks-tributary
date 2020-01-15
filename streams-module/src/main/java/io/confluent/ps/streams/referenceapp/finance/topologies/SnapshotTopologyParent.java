package io.confluent.ps.streams.referenceapp.finance.topologies;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetAggregation;
import io.confluent.ps.streams.referenceapp.utils.KSUtils;
import io.confluent.ps.streams.referenceapp.utils.WallClockProvider;
import lombok.Setter;
import org.apache.kafka.streams.kstream.KStream;

import javax.inject.Inject;
import java.time.Instant;

/**
 * Main class that houses all the Topologies and the processing logic. Two different functions are performed here:
 * - latest price recording per window, and publish to subscribers
 * - precomputed collections of latest prices {@link SnapshotSetAggregation}
 * The precomputation is demonstrated in two different styles
 * - simple version which causes the snap sets to be housed on a single node
 * - a distributed version which may be useful where the snap sets become very large (large numbers of {@link InstrumentId}s.
 * <p>
 * TODO isolate the topology functions and _unit_ test them
 *
 * @See child classes
 */
public class SnapshotTopologyParent {

  final static public String INSTRUMENTS_TOPIC_NAME = "instruments";
  final static public String FINAL_SNAP_VALUES_TOPIC_NAME = "finalWindowValues";

  @Inject
  protected SnapshotTopologySettings settings;

  @Inject
  protected KSUtils ksutils;

  @Inject
  KStream<InstrumentId, InstrumentTickBD> instrumentStream;

  @Inject
  protected SnapshotStoreProvider stores;

  @Setter
  private WallClockProvider wallClockProvider = new WallClockProvider() {
    @Override
    public Instant getNow() {
      return Instant.now();
    }
  };

  protected Instant getNow() {
    return wallClockProvider.getNow();
  }

}
