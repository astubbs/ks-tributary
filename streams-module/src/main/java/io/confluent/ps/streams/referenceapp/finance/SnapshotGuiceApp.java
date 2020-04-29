package io.confluent.ps.streams.referenceapp.finance;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.confluent.ps.streams.referenceapp.finance.modules.SnapshotModule;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/**
 * Main entry class for app, uses Dagger static injection.
 */
@Slf4j
public class SnapshotGuiceApp {

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new SnapshotModule());

    // starts the app
    SnapshotAppKafkaStream instance =
        injector.getInstance(SnapshotAppKafkaStream.class);
  }

  public static class SnapshotAppKafkaStream {

    @Inject
    SnapshotAppKafkaStream(Topology t, Properties config) {
      val ks = new KafkaStreams(t, config);

      ks.start();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          ks.close();
        }
      }));
    }
  }
}
