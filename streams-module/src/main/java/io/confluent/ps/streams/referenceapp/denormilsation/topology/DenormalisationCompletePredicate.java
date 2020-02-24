package io.confluent.ps.streams.referenceapp.denormilsation.topology;

import org.apache.kafka.streams.kstream.Predicate;
import org.checkerframework.checker.units.qual.K;

public class  DenormalisationCompletePredicate<K,V> implements Predicate<K,V> {

  @Override
  public boolean test(K key, V value) {
    return false;
  }
}
