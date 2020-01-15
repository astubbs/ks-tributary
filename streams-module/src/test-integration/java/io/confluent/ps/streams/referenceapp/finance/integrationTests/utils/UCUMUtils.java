package io.confluent.ps.streams.referenceapp.finance.integrationTests.utils;

import systems.uom.quantity.Information;
import systems.uom.quantity.InformationRate;
import tech.units.indriya.ComparableQuantity;
import tech.units.indriya.quantity.Quantities;

import javax.measure.Unit;

import static javax.measure.MetricPrefix.KILO;
import static systems.uom.ucum.UCUM.*;

/**
 * Units of measure utilities
 */
public class UCUMUtils {

  static public ComparableQuantity<Information> ofQuantity(int value, Unit<Information> aByte) {
    return Quantities.getQuantity(value, aByte);
  }

  static public ComparableQuantity<InformationRate> ofQuantityRate(int value, Unit<InformationRate> aByteRate) {
    return Quantities.getQuantity(value, aByteRate);
  }

  static public ComparableQuantity<Information> ofKiloBytes(int value) {
    return ofQuantity(value, KILO(BYTE));
  }

  static public ComparableQuantity<InformationRate> ofKiloBytesPerSec(int magnitude) {
    return ofQuantityRate(magnitude, BAUD);
  }

  static public ComparableQuantity<Information> ofBytes(int value) {
    return ofQuantity(value, BYTE);
  }
}
