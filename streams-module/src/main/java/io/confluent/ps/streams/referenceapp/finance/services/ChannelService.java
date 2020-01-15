package io.confluent.ps.streams.referenceapp.finance.services;

import io.confluent.ps.streams.referenceapp.finance.channels.MockChannels;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;

import java.util.List;

/**
 * For sake of time, this is a stub implementation of a ChannelService that would return a list of subscribed channels. Actually calling through to these methods is an error.
 */
public class ChannelService {

  /**
   * Return a list of channels subscribed to this snapset on this instance
   *
   * @param snapshotSetId
   * @return
   */
  public List<MockChannels> findChannelSubscriptions(SnapshotSetId snapshotSetId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Users could subscribe directly to keys, instead of snaps
   *
   * @param key
   * @return
   */
  public List<MockChannels> findExplicitChannelSubscriptionsForKey(InstrumentId key) {
    throw new UnsupportedOperationException();
  }
}
