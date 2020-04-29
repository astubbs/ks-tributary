package io.confluent.ps.streams.referenceapp.finance.dagger;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class BaseDaggerTest {

  @TempDir Path tempDir; // junit requires non-private

  protected DaggerSnapAppTestRoot
      .SnapshotAppCompTestComponent snapAppCompTestComponent;

  @BeforeEach
  public void setupDagger() {
    // setup dagger
    // can't automatically create modules because we inject junit path
    DaggerDaggerSnapAppTestRoot_SnapshotAppCompTestComponent.Builder builder =
        DaggerDaggerSnapAppTestRoot_SnapshotAppCompTestComponent.builder();
    builder.daggerSnapAppTestModule(new DaggerSnapAppTestModule(tempDir));
    builder.snapshotDaggerModule(new SnapshotDaggerModule());
    this.snapAppCompTestComponent = builder.build();
  }
}
