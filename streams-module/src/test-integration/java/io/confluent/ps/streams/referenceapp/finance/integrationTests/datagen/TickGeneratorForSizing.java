package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.finance.avro.idlmodel.forSizingTests.SizingInstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.finance.avro.idlmodel.forSizingTests.SizingInstrumentTick;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class TickGeneratorForSizing {

  @Inject
  TickGenerator gen;

  @Inject
  InstrumentGenerator igen;

  public List<SizingInstrumentId> constructInstruments() {
    List<InstrumentId> instrumentIds = igen.constructInstruments();
    return instrumentIds.stream().map(x -> new SizingInstrumentId(x.getId())).collect(toUnmodifiableList());
  }

  public List<SizingInstrumentTick> constructTicks(int i) {
    List<InstrumentId> ids1 = igen.constructInstruments();
    Stream<SizingInstrumentTick> sizingInstrumentTickStream = gen.constructTicks(i, ids1).stream().map(x -> new SizingInstrumentTick(new SizingInstrumentId(x.getId().getId()), x.getBloombergTimestamp(), Integer.parseInt(x.getPrice())));
    return sizingInstrumentTickStream.collect(toUnmodifiableList());
  }
}
