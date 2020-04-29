package io.confluent.ps.streams.referenceapp.finance;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import dagger.Lazy;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import java.util.Map;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public class TestBase extends GuiceInjectedTestBase {

  @Inject protected TestDataDriver tdd;

  @Inject protected TestData td;

  @Inject Lazy<TopologyTestDriver> testDriver;

  @BeforeEach
  public void setup() {
    // init services
    Map<String, StateStore> allStateStores =
        testDriver.get().getAllStateStores();
    assertThat(allStateStores.entrySet())
        .as("all state stores are actually pressent")
        .hasSize(8);

    // config
    tdd.insertConfigsData();

    if (log.isTraceEnabled()) {
      Topology topology = injector.getInstance(Topology.class);
      log.trace(topology.describe().toString());
    }
  }

  @AfterEach
  public void tearDown() {
    try {
      testDriver.get().close(); // turn off for annoying error message that
                                // doesn't matter for test
    } catch (final RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when
      // executed in Windows, ignoring it Logged stacktrace cannot be avoided
      log.warn(
          "Ignoring exception, test failing in Windows due this exception: {}",
          e.getLocalizedMessage());
    }
  }
}
