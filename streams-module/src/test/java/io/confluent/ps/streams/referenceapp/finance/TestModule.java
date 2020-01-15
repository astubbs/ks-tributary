package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceIndividual;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSharded;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSimple;
import io.confluent.ps.streams.referenceapp.finance.modules.SnapshotModule;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotStoreProvider;
import dagger.Lazy;
import dagger.internal.DoubleCheck;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Slf4j
public class TestModule extends AbstractModule {

  private Path tempDir;

  public TestModule(Path tempDir) {
    this.tempDir = Objects.requireNonNull(tempDir);
  }

  @Override
  protected void configure() {
    install(new SnapshotModule());
    bind(SnapshotStoreProvider.class).to(TestStoresProvider.class);
//
//    /**
//     * Because in {@link org.junit.jupiter.params.ParameterizedTest}, {@link org.junit.jupiter.params.provider.MethodArgumentsProvider} requires the method to be static.
//     *
//     * @see ChannelTests#checkWindowClosingParameters()
//     */
//    requestStaticInjection(ChannelTests.class);
  }

  @Provides
  @Singleton
  Lazy<TopologyTestDriver> testDriverLazy(TopologyTestDriver ttd) {
    Lazy<TopologyTestDriver> lazy = DoubleCheck.lazy((Provider) () -> ttd);
    return lazy;
  }

  @Provides
  @Singleton
  TopologyTestDriver testDriver(Topology topology) {
    Properties config = SnapshotModule.config;
    config.setProperty(StreamsConfig.STATE_DIR_CONFIG, tempDir.toString());
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, config);
    return topologyTestDriver;
  }

  @Singleton
  @Provides
  private MockChannelServiceSharded getShardChannel(TestData td) {
    MockChannelServiceSharded mockChannelService = mock(MockChannelServiceSharded.class);
    when(mockChannelService.findChannelSubscriptions(td.snapSetConfigOne.getId())).thenReturn(List.of(td.channelOneSharded, td.channelTwoSharded));
    when(mockChannelService.findChannelSubscriptions(td.snapSetConfigTwo.getId())).thenReturn(List.of(td.channelTwoSharded, td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }

  @Singleton
  @Provides
  private MockChannelServiceSimple getSimpleChannel(TestData td) {
    MockChannelServiceSimple mockChannelService = mock(MockChannelServiceSimple.class);
    when(mockChannelService.findChannelSubscriptions(td.snapSetConfigOne.getId())).thenReturn(List.of(td.channelOneSimple, td.channelTwoSimple));
    when(mockChannelService.findChannelSubscriptions(td.snapSetConfigTwo.getId())).thenReturn(List.of(td.channelTwoSimple, td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }

  @Singleton
  @Provides
  private MockChannelServiceIndividual getIndividualChannel(TestData td) {
    MockChannelServiceIndividual mockChannelService = mock(MockChannelServiceIndividual.class);
    when(mockChannelService.findExplicitChannelSubscriptionsForKey(td.googleInstrumentThreeId)).thenReturn(List.of(td.channelThreeIndividualKey, td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }

}
