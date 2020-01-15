package io.confluent.ps.streams.referenceapp.finance.model;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ModelSanityTests {

  @Test
  void testSubclassEquality() {
    InstrumentTick instrumentTick = new InstrumentTick();
    InstrumentTickBD instrumentTickBD = new InstrumentTickBD();
    assertThat(instrumentTick).as("subclass not equal to superclass regardless").isNotEqualTo(instrumentTickBD);
  }

}
