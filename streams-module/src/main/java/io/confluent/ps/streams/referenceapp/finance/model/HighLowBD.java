package io.confluent.ps.streams.referenceapp.finance.model;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.HighLow;

import javax.annotation.Nonnull;

public class HighLowBD extends HighLow {

  public HighLowBD() {
    super();
  }

  public HighLowBD(@Nonnull HighLow wrapped) {
    super(wrapped.getHigh(), wrapped.getLow());
  }

  public InstrumentTickBD getHighTick() {
    return InstrumentTickBD.of(super.getHigh());
  }

  public InstrumentTickBD getLowTick() {
    return InstrumentTickBD.of(super.getLow());
  }
}
