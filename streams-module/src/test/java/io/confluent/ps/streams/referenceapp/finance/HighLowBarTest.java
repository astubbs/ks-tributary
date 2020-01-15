package io.confluent.ps.streams.referenceapp.finance;

import com.github.jukkakarvanen.kafka.streams.test.TestRecord;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.HighLow;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.InstrumentTickBD;
import io.confluent.ps.streams.referenceapp.tests.GuiceInjectedTestBase;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologyHighLowBarWindows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
// TODO Example tests, need fleshing out
public class HighLowBarTest extends GuiceInjectedTestBase {

  @Inject
  TestDataDriver tdd;

  @Inject
  TestData td;

  @Inject
  SnapshotTopologyHighLowBarWindows hlb;

  @Test
  void highLowTestOneMinute() {
    tdd.insertTestDataSetOneHighFrequencyInOrder();
    List<TestRecord<InstrumentId, InstrumentTickBD>> dataStream = tdd.getHighFrequencyDataStream();

    //
    TestRecord<InstrumentId, InstrumentTickBD> tickZero = dataStream.get(0);
    HighLow highLowOneMinute = hlb.findHighLowOneMinute(td.baseTime.plus(ofMinutes(1)), tickZero.getKey()).get();
    Assertions.assertThat(highLowOneMinute.getHigh().getId()).isEqualTo(tickZero.getKey());
    Assertions.assertThat(highLowOneMinute.getHigh().getPrice()).isEqualTo(dataStream.get(2).getValue().getPrice());
    Assertions.assertThat(highLowOneMinute.getLow().getId()).isEqualTo(tickZero.getKey());
    Assertions.assertThat(highLowOneMinute.getLow().getPrice()).isEqualTo(tickZero.getValue().getPrice());

    HighLow highLowOneMinuteThree = hlb.findHighLowOneMinute(td.baseTime.plus(ofMinutes(1)), dataStream.get(1).getKey()).get();
    Assertions.assertThat(highLowOneMinuteThree.getHigh().getId()).isEqualTo(td.googleInstrumentThreeId);
    Assertions.assertThat(highLowOneMinuteThree.getHigh().getPrice()).isEqualTo(dataStream.get(3).getValue().getPrice());
    Assertions.assertThat(highLowOneMinuteThree.getLow().getPrice()).isEqualTo(dataStream.get(1).getValue().getPrice());

    assertThat(hlb.findHighLowOneMinute(td.baseTime.plus(ofMinutes(20)), td.appleInstrumentOneId)).isEmpty();
  }

  @Test
  void highLowTestTenMinute() {
    tdd.insertTestDataSetOneHighFrequencyInOrder();
    List<TestRecord<InstrumentId, InstrumentTickBD>> dataStream = tdd.getHighFrequencyDataStream();

    HighLow highLowOneMinute = hlb.findHighLowTenMinute(td.baseTime.plus(ofMinutes(10)), td.appleInstrumentOneId).get();
    Assertions.assertThat(highLowOneMinute.getHigh().getId()).isEqualTo(td.appleInstrumentOneId);
    Assertions.assertThat(highLowOneMinute.getHigh().getPrice()).isEqualTo(dataStream.get(8).getValue().getPrice());
    Assertions.assertThat(highLowOneMinute.getLow().getPrice()).isEqualTo(dataStream.get(9).getValue().getPrice());

    Optional<HighLow> highLowTenMinute = hlb.findHighLowTenMinute(td.baseTime.plus(ofMinutes(20)), td.appleInstrumentOneId);
    assertThat(highLowTenMinute).isPresent();
    Assertions.assertThat(highLowTenMinute.get().getHigh().getPrice()).isEqualTo(dataStream.get(10).getValue().getPrice());

    assertThat(hlb.findHighLowTenMinute(td.baseTime.plus(ofMinutes(30)), td.appleInstrumentOneId)).isEmpty();
  }

}
