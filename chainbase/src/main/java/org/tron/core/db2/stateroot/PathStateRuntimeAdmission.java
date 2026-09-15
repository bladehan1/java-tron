package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Read-only startup admission that keeps the disabled path free of filesystem access. */
public final class PathStateRuntimeAdmission {

  private PathStateRuntimeAdmission() {
  }

  public static Result inspect(boolean enabled, Path directory, Engine engine) throws IOException {
    if (!enabled) {
      return new Result(Status.DISABLED, null);
    }
    Path root = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    Engine selected = Objects.requireNonNull(engine, "engine");
    if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
      return new Result(Status.REBUILD_REQUIRED, null);
    }
    PathStateStoreManifest manifest = PathStateStoreManifest.validateExisting(root, selected);
    Status status = new PathStateCurrentStore(manifest).isInitialized()
        ? Status.CURRENT_READY : Status.REBUILD_REQUIRED;
    return new Result(status, manifest);
  }

  public enum Status {
    DISABLED,
    REBUILD_REQUIRED,
    CURRENT_READY
  }

  public static final class Result {

    private final Status status;
    private final PathStateStoreManifest manifest;

    private Result(Status status, PathStateStoreManifest manifest) {
      this.status = status;
      this.manifest = manifest;
    }

    public Status getStatus() {
      return status;
    }

    public PathStateStoreManifest getManifest() {
      return manifest;
    }
  }
}
