package io.confluent.ps.streams.referenceapp.finance;

import dagger.Component;
import io.confluent.ps.streams.referenceapp.finance.dagger.KSBindingsModule;
import io.confluent.ps.streams.referenceapp.finance.dagger.SnapshotDaggerModule;
import io.confluent.ps.streams.referenceapp.finance.dagger.StoreProdiverModule;
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
public class SnapshotDaggerApp {

  public static void main(String[] args) {
    SnapshotAppComp snapshotAppComp =
        DaggerSnapshotDaggerApp_SnapshotAppComp.create();
    SnapshotAppKafkaStream maker = snapshotAppComp.maker();

    log.info(snapshotAppComp.toString());
    log.info(maker.toString());
  }

  @Singleton
  @Component(modules = {KSBindingsModule.class, SnapshotDaggerModule.class,
                        StoreProdiverModule.class})
  public interface SnapshotAppComp {
    SnapshotAppKafkaStream maker();
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
