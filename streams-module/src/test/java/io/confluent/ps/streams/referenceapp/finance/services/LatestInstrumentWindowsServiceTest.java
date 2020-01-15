package io.confluent.ps.streams.referenceapp.finance.services;

import io.confluent.ps.streams.referenceapp.finance.TestData;
import io.confluent.ps.streams.referenceapp.finance.TestDataDriver;
import io.confluent.ps.streams.referenceapp.finance.dagger.BaseDaggerTest;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class LatestInstrumentWindowsServiceTest extends BaseDaggerTest {

  @Test
  void test() {
    // get instance
    TestData td = snapAppCompTestComponent.testData();
    TestDataDriver tdd = snapAppCompTestComponent.testDataDriver();

    tdd.insertTestDataSetOne();
    tdd.insertAllTestDataInOrder();

    LatestInstrumentWindowsService latestInstrumentWindowsService = snapAppCompTestComponent.latestService();
    ArrayList<KeyValue<InstrumentId, InstrumentTickBD>> ticksForAllInstrumentWindowsEndingAt = latestInstrumentWindowsService.findTicksForAllInstrumentWindowsEndingAt(td.snapTime);

    assertThat(ticksForAllInstrumentWindowsEndingAt).hasSize(3);
  }
}
