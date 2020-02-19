package io.confluent.ps.streams.referenceapp.schools.topology;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@RequiredArgsConstructor
public class KSStoreProvider implements StoreProvider {
  final KafkaStreams ks;

  @Override
  public <T> ReadOnlyKeyValueStore getStore(String name, QueryableStoreType<T> queryableStoreType) {
    return (ReadOnlyKeyValueStore) ks.store(name, queryableStoreType);
  }
}