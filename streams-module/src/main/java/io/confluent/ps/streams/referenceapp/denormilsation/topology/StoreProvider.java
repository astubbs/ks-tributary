package io.confluent.ps.streams.referenceapp.denormilsation.topology;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public interface StoreProvider {
  <T> ReadOnlyKeyValueStore getStore(String name, QueryableStoreType<T> queryableStoreType);
}


