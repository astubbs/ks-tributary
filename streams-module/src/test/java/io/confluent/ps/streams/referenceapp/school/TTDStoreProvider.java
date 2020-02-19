package io.confluent.ps.streams.referenceapp.school;

import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;
import io.confluent.ps.streams.referenceapp.denormilsation.topology.StoreProvider;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.inject.Inject;
import javax.inject.Provider;

public class TTDStoreProvider implements StoreProvider {

  final Provider<TopologyTestDriver> tdd;

  @Inject
  public TTDStoreProvider(Provider<TopologyTestDriver> tdd) {
    this.tdd = tdd;
  }

  @Override
  public <T> ReadOnlyKeyValueStore getStore(String name, QueryableStoreType<T> queryableStoreType) {
    return tdd.get().getKeyValueStore(name);
  }

}
