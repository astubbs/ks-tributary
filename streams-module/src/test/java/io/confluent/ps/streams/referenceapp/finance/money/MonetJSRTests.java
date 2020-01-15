package io.confluent.ps.streams.referenceapp.finance.money;

import io.confluent.ps.streams.referenceapp.finance.currency.Currencies;
import lombok.val;
import org.javamoney.moneta.Money;
import org.javamoney.moneta.spi.DefaultNumberValue;
import org.junit.jupiter.api.Test;

import javax.money.MonetaryException;
import javax.money.NumberValue;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MonetJSRTests {

  @Test
  void basic() {

    val gbp = Money.of(12.3, "GBP");
    val hdk = Money.of(1.45, "HKD");

    assertThatThrownBy(() -> gbp.add(hdk))
            .isInstanceOf(MonetaryException.class)
            .hasMessageContaining("mismatch")
            .hasMessageContainingAll(
                    hdk.getCurrency().getCurrencyCode(),
                    gbp.getCurrency().getCurrencyCode()
            );
  }

  @Test
  void minorUnits(){
    Money money = Money.ofMinor(Currencies.nzdCur, 150);
    assertThat(money.getNumberStripped()).isEqualByComparingTo("1.5");
    NumberValue number = money.getNumber();
    assertThat(number.getNumberType()).isEqualTo(BigDecimal.class);
    assertThat(number.getPrecision()).isEqualTo(2);
    assertThat(number.getScale()).isEqualTo(1);
    assertThat(number.doubleValue()).isEqualTo(1.5);
    assertThat(number.doubleValueExact()).isEqualTo(1.5);
    assertThat(number.intValue()).isEqualTo(1);
    assertThatThrownBy(()->number.intValueExact()).isInstanceOf(ArithmeticException.class);
    assertThat(number).isEqualByComparingTo(DefaultNumberValue.of(1.50));
  }

}
