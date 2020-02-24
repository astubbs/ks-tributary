package io.confluent.ps.connect.etod;

import avro.shaded.com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class EtodConversionTest {

  public static final String ZSERIES_TOD_1970 = "7D91048BCA000";
  public static final String TOD_PROGRAMMABLE_NULL_FIELD = "000";
  int extendedByteWidthSize = 128 / 4;
  int standardByteWidthSize = 64 / 4;

  @Test
  void todConversion() {
    val data = Lists.newArrayList(
            Pair.of(ZSERIES_TOD_1970 + TOD_PROGRAMMABLE_NULL_FIELD, "1970-01-01T00:00:00.000Z"),
            Pair.of("CAE7631DC43DC686", "2013-02-10T21:59:46.420Z"),
            null // terminator
    );

    data.stream().forEach((x) -> {
      if (x == null) return;
      val input = x.getLeft();
      assertThat(input).hasSize(standardByteWidthSize);
      val y = convertTOD(input).toString();
      log.info(y.toString());
      val expected = x.getRight();
      assertThat(y).isEqualTo(expected);
    });
  }

  @Test
  void extendedTodConversion() {
    val data = Lists.newArrayList(
            Pair.of("00D77493B5DC87BB2C000000060C0005", "2020-02-10T13:45:09:1581342309"),
//            "00D77493B94112285400000006000006",
//            "00D774941087167E0400000006180006",
//            "00D774A01942DBB33A000000063C0006",
            Pair.of("", "") // terminator
    );

    data.stream().forEach((x) -> {
      if (x == null) return;
      val input = x.getLeft();
      assertThat(input).hasSize(extendedByteWidthSize);
      val y = convertETOD(input).toString();
      log.info(y.toString());
      val expected = x.getRight();
      assertThat(y).isEqualTo(expected);
    });
  }

  private DateTime convertETOD(String x) {
    val halfPoint = extendedByteWidthSize / 2;
    String highOrder = x.substring(0, halfPoint);
    String lowOrder = x.substring(halfPoint);
    return convertTOD(highOrder);
  }

  /**
   * https://stackoverflow.com/questions/14817202/tod-clock-time-to-java-util-date-or-milliseconds#32755423
   *
   * @return
   */
  DateTime convertTOD(String input) {
    assertThat(input).hasSize(16); // standard TOD must be 16 hex chars wide (words)

    // we start with your string minus the three last digits
    // which are some internal z/Series cruft
    BigInteger bi = new BigInteger(input, 16);
    bi = bi.divide(new BigInteger("1000", 16)); // remove last 12 bits (programmable section)

    // then, from tables the website we get the TOD value for start of epoch
    // here also, minus the three last digits
    BigInteger startOfEpoch70 = new BigInteger(ZSERIES_TOD_1970, 16); // 000 stripped off
    // using that we calculate the offset in microseconds in epoch
    BigInteger microsinepoch = bi.subtract(startOfEpoch70);
    // and reduce to millis
    BigInteger millisinepoch = microsinepoch.divide(new BigInteger("1000"));
    // which we convert to a long to feed to Joda
    long millisinepochLong = millisinepoch.longValue();
    // Et voila, the result in UTC
    DateTime result = new DateTime(millisinepochLong).withZone(DateTimeZone.UTC);

    return result;

  }

  void convertToEasternEuropeanTZ(DateTime input) {
    // Now, if you want a result in some other timezone, that's equally easy with Joda:
    String easternEuropeanTime = "EET";
    DateTime eetTimeZoneConverted = input.toDateTime(DateTimeZone.forID(easternEuropeanTime));

    System.out.println("The result is " + input + " or represented in timezone EET "
            + eetTimeZoneConverted);
  }
}
