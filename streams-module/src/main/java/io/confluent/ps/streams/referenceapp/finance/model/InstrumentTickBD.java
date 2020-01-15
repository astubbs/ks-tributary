package io.confluent.ps.streams.referenceapp.finance.model;

import io.confluent.ps.streams.referenceapp.finance.currency.Currencies;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentTick;
import lombok.NoArgsConstructor;
import org.javamoney.moneta.Money;

import javax.annotation.Nonnull;
import javax.money.CurrencyUnit;
import java.math.BigDecimal;
import java.time.Instant;

@NoArgsConstructor
public class InstrumentTickBD extends InstrumentTick {

  static private CurrencyUnit defaultCurrency = Currencies.defaultCurrenty;

  public InstrumentTickBD(InstrumentId id, Long bloombergTimestamp, String price, String currencyCode) {
    super(id, bloombergTimestamp, price, currencyCode);
  }

  public InstrumentTickBD(@Nonnull InstrumentTick id) {
    super(id.getId(), id.getBloombergTimestamp(), id.getPrice(), id.getCurrencyCode());
  }

  public static InstrumentTickBD of(InstrumentId id, String price, Instant tickTime) {
    return InstrumentTickBD.of(id, price, tickTime.toEpochMilli());
  }

  public static InstrumentTickBD of(InstrumentId id, String price, Long tickTime) {
    return new InstrumentTickBD(id, tickTime, price, defaultCurrency.getCurrencyCode());
  }

  public static InstrumentTickBD of(InstrumentTick price) {
    if (price == null)
      return null;
    return new InstrumentTickBD(price.getId(), price.getBloombergTimestamp(), price.getPrice(), price.getCurrencyCode());
  }

  public Money getPriceMoney() {
    String price = super.getPrice();
    BigDecimal bigDecimal = new BigDecimal(price);
    Money of = Money.of(bigDecimal, defaultCurrency);
    return of;
  }

  /**
   * Not true if equal. True when compared to null.
   */
  public boolean isPriceGreaterThan(InstrumentTickBD other) {
    if (other == null) return true;
    return this.getPriceMoney().isGreaterThan(other.getPriceMoney());
  }

  /**
   * Not true if equal. True when compared to null.
   */
  public boolean isPriceLesserThan(InstrumentTickBD other) {
    if (other == null) return true;
    return this.getPriceMoney().isLessThan(other.getPriceMoney());
  }

}
