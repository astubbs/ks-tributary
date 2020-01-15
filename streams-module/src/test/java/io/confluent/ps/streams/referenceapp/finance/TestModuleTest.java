package io.confluent.ps.streams.referenceapp.finance;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannelServiceIndividual;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TestModuleTest {

  @TempDir
  Path tempDir;

  @Test
  void singletonChannels() {
    Injector injector = Guice.createInjector(new TestModule(tempDir));

    MockChannelServiceIndividual csIndividualClientsOne = injector.getBinding(MockChannelServiceIndividual.class).getProvider().get();
    MockChannelServiceIndividual csIndividualClientsTwo = injector.getBinding(MockChannelServiceIndividual.class).getProvider().get();
    assertThat(csIndividualClientsOne).as("Working singleton system").isSameAs(csIndividualClientsTwo);
  }
}
