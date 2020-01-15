package io.confluent.ps.streams.referenceapp.finance.channels;

import io.confluent.ps.streams.referenceapp.finance.services.ChannelService;

import javax.inject.Inject;


public class MockChannelServiceSharded extends ChannelService {

  @Inject
  public MockChannelServiceSharded() {
  }

}
