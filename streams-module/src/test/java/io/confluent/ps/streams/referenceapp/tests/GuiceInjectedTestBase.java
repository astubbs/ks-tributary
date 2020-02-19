package io.confluent.ps.streams.referenceapp.tests;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.confluent.ps.streams.referenceapp.finance.TestModule;
import io.confluent.ps.streams.referenceapp.school.SchoolTestModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class GuiceInjectedTestBase {

  protected Injector injector;

  @TempDir
  Path tempDir; // junit requires non-private

  @BeforeEach
  public void inject() {
    injector = Guice.createInjector(new TestModule(tempDir), new SchoolTestModule());
    injector.injectMembers(this);
  }

}