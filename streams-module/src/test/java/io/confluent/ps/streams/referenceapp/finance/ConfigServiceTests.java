package io.confluent.ps.streams.referenceapp.finance;

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentsToSnapshotSets;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetKeys;
import io.confluent.ps.streams.referenceapp.finance.services.SnapshotSetsConfigService;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;


public class ConfigServiceTests extends TestBase {

  @Inject
  protected SnapshotSetsConfigService configService;

  @Inject
  protected TestData td;

  @Test
  void loadSimpleConfig() {
    // test config lookup
    List<InstrumentId> keySetForSnap = configService.getKeySetForSnap(td.snapSetConfigOne.getId());
    List<InstrumentId> instruments = td.snapSetConfigOne.getInstruments();
    assertThat(keySetForSnap).containsAll(instruments);

    //
    ImmutableList<KeyValue<SnapshotSetId, ValueAndTimestamp<SnapshotSetKeys>>> allConfigs = ImmutableList.copyOf(configService.getAllConfigs());
    assertThat(allConfigs).hasSize(2);
  }

  @Test
  void testSlowScanTechniqueInstrumentToSnapLookup() {
    // test inverse lookup instruments -> snapset
    List<SnapshotSetId> allMatchingSnapsetsForKey = configService.findAllMatchingSnapsetsForKeySlowScanTechnique(td.appleInstrumentOneId);
    assertThat(allMatchingSnapsetsForKey).containsExactlyInAnyOrder(td.snapSetConfigOne.getId(), td.snapSetConfigTwo.getId());

    assertThat(configService.findAllMatchingSnapsetsForKeySlowScanTechnique(td.googleInstrumentThreeId)).as("doesn't contain").hasSize(0);

    assertThat(configService.findAllMatchingSnapsetsForKeySlowScanTechnique(td.starbucksInstrumentThreeId)).hasSize(1);
  }

  @Test
  void testFastTechniqueInstrumentToSnapLookup() {
    // test inverse lookup instruments -> snapset
    Optional<InstrumentsToSnapshotSets> allMatchingSnapsetsForKey = configService.findAllMatchingSnapsetsForKeyFastInverseTechnique(td.appleInstrumentOneId);
    assertThat(allMatchingSnapsetsForKey).get();
    assertThat(allMatchingSnapsetsForKey.get().getSnapSets()).containsExactlyInAnyOrder(td.snapSetConfigOne.getId(), td.snapSetConfigTwo.getId());
  }

  @Test
  void loadAllInverses() {
    List<KeyValue<InstrumentId, ValueAndTimestamp<InstrumentsToSnapshotSets>>> allConfigs = ImmutableList.copyOf(configService.getAllInverseConfigs());
    assertThat(allConfigs).hasSize(2);
  }

  /**
   * At the moment, hew configs will require time to roll over before precomputed sets take affect - e.g. dynamic reconfiguration isn't supported, you just have to wait for the the window time to pass, before the precomputed snaps will match the configurations.
   */
  @Test
  @Disabled("New configs should trigger recalculation of snap sets")
  void testReconfiguration() {
  }

}
