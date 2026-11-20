package org.tron.core.vm.program;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.core.store.StorageRowStore;

public class Storage {

  /** Loads a physical storage row from the current Store or one historical request. */
  @FunctionalInterface
  public interface RowLoader {
    StorageRowCapsule get(byte[] physicalKey);
  }

  @Getter
  private final Map<DataWord, StorageRowCapsule> rowCache = new HashMap<>();
  @Getter
  private byte[] addrHash;
  @Getter
  private StorageRowStore store;
  private final RowLoader rowLoader;
  @Getter
  private byte[] address;
  @Setter
  private int contractVersion;

  public Storage(byte[] address, StorageRowStore store) {
    this(address, store, key -> store.get(key));
  }

  /** Uses a session-bound loader; a null Store prevents historical writes from committing. */
  public Storage(byte[] address, StorageRowStore store, RowLoader rowLoader) {
    addrHash = addrHash(address);
    this.address = address;
    this.store = store;
    this.rowLoader = rowLoader;
  }

  public Storage(Storage storage) {
    this.addrHash = storage.addrHash.clone();
    this.address = storage.getAddress().clone();
    this.store = storage.store;
    this.rowLoader = storage.rowLoader;
    this.contractVersion = storage.contractVersion;
    storage.getRowCache().forEach((DataWord rowKey, StorageRowCapsule row) -> {
      StorageRowCapsule newRow = new StorageRowCapsule(row);
      this.rowCache.put(rowKey.clone(), newRow);
    });
  }

  private byte[] compose(byte[] key, byte[] addrHash) {
    return StorageRowKeyCodec.physicalKeyFromAddressHash(addrHash, key, contractVersion);
  }

  private static byte[] addrHash(byte[] address) {
    return StorageRowKeyCodec.addressHash(address, null);
  }

  private static byte[] addrHash(byte[] address, byte[] trxHash) {
    return StorageRowKeyCodec.addressHash(address, trxHash);
  }

  public void generateAddrHash(byte[] trxId) {
    // update addreHash for create2
    addrHash = addrHash(address, trxId);
  }

  public DataWord getValue(DataWord key) {
    if (rowCache.containsKey(key)) {
      return new DataWord(rowCache.get(key).getValue());
    } else {
      StorageRowCapsule row = rowLoader.get(compose(key.getData(), addrHash));
      if (row == null || row.getInstance() == null) {
        return null;
      }
      rowCache.put(key, row);
      return new DataWord(row.getValue());
    }
  }

  public void put(DataWord key, DataWord value) {
    if (rowCache.containsKey(key)) {
      rowCache.get(key).setValue(value.getData());
    } else {
      byte[] rowKey = compose(key.getData(), addrHash);
      StorageRowCapsule row = new StorageRowCapsule(rowKey, value.getData());
      rowCache.put(key, row);
    }
  }

  public void commit() {
    if (store == null) {
      throw new IllegalStateException("Read-only historical storage cannot be committed");
    }
    rowCache.forEach((DataWord rowKey, StorageRowCapsule row) -> {
      if (row.isDirty()) {
        if (new DataWord(row.getValue()).isZero()) {
          this.store.delete(row.getRowKey());
        } else {
          this.store.put(row.getRowKey(), row);
        }
      }
    });
  }
}
