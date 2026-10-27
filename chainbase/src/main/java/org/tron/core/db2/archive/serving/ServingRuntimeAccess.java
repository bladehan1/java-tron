package org.tron.core.db2.archive.serving;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Runtime assembly boundary; concrete mutable builders and native handles stay in serving. */
public final class ServingRuntimeAccess {

  private ServingRuntimeAccess() {}

  /** Stages already made durable by the catalog before notifying its runtime owner. */
  public enum PublicationStage {
    GENERATION_INSTALLED,
    CURRENT_PUBLISHED
  }

  @FunctionalInterface
  public interface PublicationObserver {
    void afterStage(PublicationStage stage) throws IOException;
  }

  /** One owned builder accepts validated logical plans and returns immutable generation pins. */
  public interface Builder extends Closeable {
    PersistentServingKeyIndexGeneration extendExact(
        Path shadow,
        String generationId,
        ServingIndexIncrementalPlan plan,
        byte[] latestSourceIdentityDigest)
        throws IOException;
  }

  public static PersistentServingKeyIndexCatalog openCatalog(
      Path root, Engine engine, PublicationObserver observer) throws IOException {
    return PersistentServingKeyIndexCatalog.open(root, engine, observerHook(observer));
  }

  public static PersistentServingKeyIndexCatalog createCatalog(
      Path root, Path initialShadow, PublicationObserver observer) throws IOException {
    return PersistentServingKeyIndexCatalog.create(root, initialShadow, observerHook(observer));
  }

  public static Builder openRuntimeBuilder(
      PersistentServingKeyIndexGeneration source, Path directory) throws IOException {
    return source.openRuntimeBuilder(directory);
  }

  public static byte[] sourceDigestForRebuild(ServingIndexIncrementalPlan plan) {
    return PersistentServingKeyIndexGeneration.sourceDigestForRebuild(plan);
  }

  private static PersistentServingKeyIndexCatalog.FaultHook observerHook(
      PublicationObserver observer) {
    Objects.requireNonNull(observer, "observer");
    return stage ->
        observer.afterStage(
            stage == PersistentServingKeyIndexCatalog.PublicationStage.GENERATION_INSTALLED
                ? PublicationStage.GENERATION_INSTALLED
                : PublicationStage.CURRENT_PUBLISHED);
  }
}
