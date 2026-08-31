package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Non-creating startup gate for the fresh physical 27+1 format. */
public final class PathStatePhysicalRuntimeAdmission {

  private PathStatePhysicalRuntimeAdmission() {
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
    PathStatePhysicalStoreManifest manifest =
        PathStatePhysicalStoreManifest.validateExisting(root, selected);
    Status status = Files.isRegularFile(root.resolve(PathStatePhysicalStoreSet.CURRENT_FILE),
        LinkOption.NOFOLLOW_LINKS) ? Status.CURRENT_CANDIDATE : Status.REBUILD_REQUIRED;
    return new Result(status, manifest);
  }

  public enum Status {
    DISABLED,
    REBUILD_REQUIRED,
    CURRENT_CANDIDATE
  }

  public static final class Result {

    private final Status status;
    private final PathStatePhysicalStoreManifest manifest;

    private Result(Status status, PathStatePhysicalStoreManifest manifest) {
      this.status = status;
      this.manifest = manifest;
    }

    public Status getStatus() {
      return status;
    }

    public PathStatePhysicalStoreManifest getManifest() {
      return manifest;
    }
  }
}
