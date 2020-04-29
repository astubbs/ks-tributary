package io.confluent.ps.streams.referenceapp.finance.dagger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import dagger.Module;
import dagger.Provides;
import io.confluent.ps.streams.referenceapp.finance.TestData;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceIndividual;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSharded;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceSimple;
import io.confluent.ps.streams.referenceapp.finance.modules.SnapshotModule;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.mockito.Mockito;

@Module
@Slf4j
public class DaggerSnapAppTestModule {

  final private Path tempDir;

  public DaggerSnapAppTestModule(Path tempDir) {
    this.tempDir = Objects.requireNonNull(tempDir);
  }

  @Provides
  @Singleton
  TopologyTestDriver testDriver(Topology topology) {
    Properties config = SnapshotModule.config;
    config.setProperty(StreamsConfig.STATE_DIR_CONFIG, tempDir.toString());
    return new TopologyTestDriver(topology, config);
  }

  @Singleton
  @Provides
  MockChannelServiceSharded getShardChannel(TestData td) {
    MockChannelServiceSharded mockChannelService =
        Mockito.mock(MockChannelServiceSharded.class);
    Mockito
        .when(mockChannelService.findChannelSubscriptions(
            td.snapSetConfigOne.getId()))
        .thenReturn(List.of(td.channelOneSharded, td.channelTwoSharded));
    Mockito
        .when(mockChannelService.findChannelSubscriptions(
            td.snapSetConfigTwo.getId()))
        .thenReturn(List.of(td.channelTwoSharded,
                            td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }

  @Singleton
  @Provides
  MockChannelServiceSimple getSimpleChannel(TestData td) {
    MockChannelServiceSimple mockChannelService =
        Mockito.mock(MockChannelServiceSimple.class);
    Mockito
        .when(mockChannelService.findChannelSubscriptions(
            td.snapSetConfigOne.getId()))
        .thenReturn(List.of(td.channelOneSimple, td.channelTwoSimple));
    Mockito
        .when(mockChannelService.findChannelSubscriptions(
            td.snapSetConfigTwo.getId()))
        .thenReturn(List.of(td.channelTwoSimple,
                            td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }

  @Singleton
  @Provides
  MockChannelServiceIndividual getIndividualChannel(TestData td) {
    MockChannelServiceIndividual mockChannelService =
        Mockito.mock(MockChannelServiceIndividual.class);
    Mockito
        .when(mockChannelService.findExplicitChannelSubscriptionsForKey(
            td.googleInstrumentThreeId))
        .thenReturn(List.of(td.channelThreeIndividualKey,
                            td.channelFiveDataArrivesAfterWindowClose));
    return mockChannelService;
  }
}
