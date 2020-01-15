package io.confluent.ps.streams.referenceapp.finance.currency;

import javax.money.CurrencyUnit;
import javax.money.Monetary;

public class Currencies {

  static final public CurrencyUnit poundCur = Monetary.getCurrency("GBP");
  static final public CurrencyUnit euroCur = Monetary.getCurrency("EUR");
  static final public CurrencyUnit usdCur = Monetary.getCurrency("USD");
  static final public CurrencyUnit hkdCur = Monetary.getCurrency("HKD");
  static final public CurrencyUnit nzdCur = Monetary.getCurrency("NZD");

  static final public CurrencyUnit defaultCurrenty = poundCur;

}
