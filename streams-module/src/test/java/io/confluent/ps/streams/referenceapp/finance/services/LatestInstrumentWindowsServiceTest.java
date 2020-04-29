package io.confluent.ps.streams.referenceapp.finance.services;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.ps.streams.referenceapp.finance.TestBase;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import java.util.ArrayList;
import javax.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

public class LatestInstrumentWindowsServiceTest extends TestBase {

  @Inject LatestInstrumentWindowsService latestInstrumentWindowsService;

  @Test
  void test() {
    tdd.insertTestDataSetOne();
    tdd.insertAllTestDataInOrder();

    ArrayList<KeyValue<InstrumentId, InstrumentTickBD>>
        ticksForAllInstrumentWindowsEndingAt =
            latestInstrumentWindowsService
                .findTicksForAllInstrumentWindowsEndingAt(td.snapTime);

    assertThat(ticksForAllInstrumentWindowsEndingAt).hasSize(3);
  }
}
