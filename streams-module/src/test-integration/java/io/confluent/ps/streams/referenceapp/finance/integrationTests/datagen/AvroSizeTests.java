package io.confluent.ps.streams.referenceapp.finance.integrationTests.datagen;

import com.github.luben.zstd.ZstdOutputStream;
import io.confluent.ps.streams.referenceapp.finance.model.finance.avro.idlmodel.forSizingTests.SizingInstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.finance.avro.idlmodel.forSizingTests.SizingInstrumentTick;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import name.falgout.jeffrey.testing.junit.guice.GuiceExtension;
import name.falgout.jeffrey.testing.junit.guice.IncludeModule;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xerial.snappy.SnappyOutputStream;
import systems.uom.quantity.Information;
import tech.units.indriya.ComparableQuantity;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static io.confluent.ps.streams.referenceapp.finance.integrationTests.utils.UCUMUtils.ofBytes;
import static io.confluent.ps.streams.referenceapp.finance.integrationTests.utils.UCUMUtils.ofKiloBytesPerSec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Micro benchmarking, but gives you an idea.
 */
@TestInstance(PER_CLASS)
@Slf4j
@IncludeModule(DataGenModule.class)
@ExtendWith(GuiceExtension.class)
//@ExtendWith(SoftAssertionsExtension.class)
public class AvroSizeTests {

  @Inject
  TickGeneratorForSizing gen;

  List<SizingInstrumentId> ids;

  @BeforeAll
  void setup() {
    ids = gen.constructInstruments();
  }

  // TODO doesn't work?
//  @Rule
//  public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

  @SneakyThrows
  @Test
  void sanityChecks() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThat(out.size()).isZero();

    out.write(0);
    assertThat(out.size()).isOne();
  }

  @SneakyThrows
  @Test
  void howBig() {
    Schema classSchema = SizingInstrumentTick.getClassSchema();

    SpecificDatumWriter<SizingInstrumentTick> writer = new SpecificDatumWriter<>(classSchema);
    EncoderFactory encoderFactory = EncoderFactory.get();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

    int totalTicks = 1000;

    SizingInstrumentTick tick = gen.constructTicks(1).stream().findFirst().get();
    assertThat(tick.toString()).as("generated the same every time")
            .isEqualTo("{\"id\": {\"id\": \"Gelton\"}, \"bloomberg_timestamp\": 395107200001, \"price\": 178}");

    writer.write(tick, encoder);
    val sizeOfFirst = ofBytes(out.size());

    ComparableQuantity<Information> expectedSizeOfFirst = ofBytes(15);
    assertThat(sizeOfFirst).isEqualTo(expectedSizeOfFirst);
    log.debug("Schema is timestamp (8 bytes), price (int 4 bytes), schema id (variable string - first tick is '{}', {} letters, 2 bytes each)", tick.getId().getId(), tick.getId().getId().length());
    log.debug("Size of first tick is {} bytes, it is: {}", expectedSizeOfFirst, tick);

    List<SizingInstrumentTick> lots = gen.constructTicks(totalTicks - 1);
    for (SizingInstrumentTick t : lots) {
      writer.write(t, encoder);
    }

    val sizeOfLots = ofBytes(out.size());
    assertThat(sizeOfLots).as("Bytes").isEqualTo(ofBytes(16115));

    Offset<Integer> offset = Offset.offset(5); // each tick contains different data, so below is just an average size
    val averageSize = sizeOfLots.divide(totalTicks);
    assertThat(averageSize.getValue().intValue()).isCloseTo(sizeOfFirst.getValue().intValue(), offset);

    val aSmallDataSizeOf500kB = ofBytes(500000);
    val quantityOfTicksThatWouldFitInHalfAMeg = aSmallDataSizeOf500kB.divide(averageSize);

    int actual = quantityOfTicksThatWouldFitInHalfAMeg.getValue().intValue();
    assertThat(actual).as("a seriously large amount").isCloseTo(31026, withPercentage(1));

    val atATransferRateOf4MBPerS = ofKiloBytesPerSec(4000000);
    val ticksPerSecond = atATransferRateOf4MBPerS.divide(averageSize);
    assertThat(ticksPerSecond.getValue().intValue()).as("a lot per second").isCloseTo(248215, withPercentage(1));
  }

  @SneakyThrows
  @RepeatedTest(3)
  void compressed() {
    Schema classSchema = SizingInstrumentTick.getClassSchema();
    SpecificDatumWriter<SizingInstrumentTick> writer = new SpecificDatumWriter<>(classSchema);
    EncoderFactory encoderFactory = EncoderFactory.get();

    //
    ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
    ByteArrayOutputStream gzOut = new ByteArrayOutputStream();
    ByteArrayOutputStream zstdOut = new ByteArrayOutputStream();
    ByteArrayOutputStream snappyOut = new ByteArrayOutputStream();

    val gzipped = new GZIPOutputStream(gzOut);
    val zstdOutputStream = new ZstdOutputStream(zstdOut);
    val snap = new SnappyOutputStream(snappyOut);

    //
    val rawEncoder = encoderFactory.directBinaryEncoder(rawOut, null);
    val gzEncoder = encoderFactory.directBinaryEncoder(gzipped, null);
    val zstdEn = encoderFactory.directBinaryEncoder(zstdOutputStream, null);
    val snapEn = encoderFactory.directBinaryEncoder(snap, null);


    //
    List<SizingInstrumentTick> lots = gen.constructTicks(100000);

    //
    for (SizingInstrumentTick i : lots) {
      writer.write(i, rawEncoder);
      writer.write(i, gzEncoder);
      writer.write(i, zstdEn);
      writer.write(i, snapEn);
    }

    // try to flush compression buffers
    gzipped.finish();
    gzipped.close();
    zstdOutputStream.close();
    snap.close();

    SoftAssertions s = new SoftAssertions();

    s.assertThat(rawOut.size()).isCloseTo(1620177, withPercentage(1.1)); // todo should this not be deterministic?
    s.assertThat(snappyOut.size()).isCloseTo(1246881, withPercentage(0.5));
    s.assertThat(gzOut.size()).isCloseTo(752493, withPercentage(0.5));
    s.assertThat(zstdOut.size()).isCloseTo(661778, withPercentage(0.5));

    // TODO use @ExtendWith(SoftAssertionsExtension.class) instead? Doens't seem to work?
    // s.assertThat(false).isTrue();

    //
    s.assertThat(snappyOut.size()).as("snappy vs raw").isNotCloseTo(rawOut.size(), withPercentage(22)); // 22% smaller
    s.assertThat(gzOut.size()).as("gz vs raw").isNotCloseTo(rawOut.size(), withPercentage(53));
    s.assertThat(zstdOut.size()).as("zstd vs raw").isNotCloseTo(rawOut.size(), withPercentage(58));

    //
    s.assertThat(zstdOut.size()).as("zstd vs snappy").isNotCloseTo(snappyOut.size(), withPercentage(46));

    //
    s.assertThat(gzOut.size()).as("gz vs zstd").isNotCloseTo(zstdOut.size(), withPercentage(12));

    //
    s.assertAll();

  }

  // TODO
  @Test
  @Disabled
  void compareWithJson() {
  }

}
