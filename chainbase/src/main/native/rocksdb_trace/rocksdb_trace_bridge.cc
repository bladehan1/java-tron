#include <jni.h>
#include <dlfcn.h>
#include <link.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "rocksdb/block_cache_trace_writer.h"
#include "rocksdb/db.h"

// rocksdbjni keeps RocksDB symbols local to its JNI shared object. The bridge invokes DB trace
// methods through the public virtual interface, but Status construction/stringification used by
// this translation unit still needs local definitions. Keep these two ABI-matched v9.7 methods
// local to this library; no DB/cache/table implementation is linked into the process.
namespace rocksdb {
Status::Status(Code code, SubCode subcode, const Slice& message, const Slice& message2,
               Severity severity)
    : code_(code), subcode_(subcode), sev_(severity), retryable_(false), data_loss_(false),
      scope_(0) {
  size_t first = message.size();
  size_t second = message2.size();
  size_t size = first + (second == 0 ? 0 : second + 2);
  char* state = new char[size + 1];
  std::memcpy(state, message.data(), first);
  if (second != 0) {
    state[first] = ':';
    state[first + 1] = ' ';
    std::memcpy(state + first + 2, message2.data(), second);
  }
  state[size] = '\0';
  state_.reset(state);
}

std::string Status::ToString() const {
  if (ok()) {
    return "OK";
  }
  return std::string("RocksDB status code=") + std::to_string(static_cast<int>(code()))
      + " subcode=" + std::to_string(static_cast<int>(subcode()))
      + (getState() == nullptr ? "" : std::string(" message=") + getState());
}
}  // namespace rocksdb

namespace {

constexpr uint64_t kReservedGetId = 0;

using StartBlockCacheTrace = rocksdb::Status (*)(
    void*, const rocksdb::BlockCacheTraceOptions&,
    std::unique_ptr<rocksdb::BlockCacheTraceWriter>&&);
using EndBlockCacheTrace = rocksdb::Status (*)(void*);

struct RocksDbTraceFunctions {
  StartBlockCacheTrace start = nullptr;
  EndBlockCacheTrace end = nullptr;
  std::string error;
};

int FindRocksDbJni(struct dl_phdr_info* info, size_t, void* data) {
  if (info->dlpi_name != nullptr
      && std::strstr(info->dlpi_name, "librocksdbjni") != nullptr) {
    *static_cast<std::string*>(data) = info->dlpi_name;
    return 1;
  }
  return 0;
}

RocksDbTraceFunctions ResolveTraceFunctions() {
  RocksDbTraceFunctions functions;
  std::string path;
  dl_iterate_phdr(FindRocksDbJni, &path);
  if (path.empty()) {
    functions.error = "loaded rocksdbjni library was not found";
    return functions;
  }
  void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
  if (handle == nullptr) {
    functions.error = std::string("unable to inspect rocksdbjni: ") + dlerror();
    return functions;
  }
  // Resolve the version-specific DBImpl methods. The public DB vtable layout can differ when
  // rocksdbjni and this bridge are compiled with different RocksDB feature macros.
  functions.start = reinterpret_cast<StartBlockCacheTrace>(dlsym(
      handle,
      "_ZN7rocksdb6DBImpl20StartBlockCacheTraceERKNS_22BlockCacheTraceOptionsEOSt10unique_ptrINS_21BlockCacheTraceWriterESt14default_deleteIS5_EE"));
  functions.end = reinterpret_cast<EndBlockCacheTrace>(
      dlsym(handle, "_ZN7rocksdb6DBImpl18EndBlockCacheTraceEv"));
  if (functions.start == nullptr || functions.end == nullptr) {
    functions.error = "RocksDB 9.7 DBImpl block-cache trace symbols were not found";
  }
  return functions;
}

const RocksDbTraceFunctions& TraceFunctions() {
  static const RocksDbTraceFunctions functions = ResolveTraceFunctions();
  return functions;
}

uint64_t Mix(uint64_t value) {
  value ^= value >> 30;
  value *= UINT64_C(0xbf58476d1ce4e5b9);
  value ^= value >> 27;
  value *= UINT64_C(0x94d049bb133111eb);
  return value ^ (value >> 31);
}

uint64_t HashSlice(const rocksdb::Slice& value) {
  uint64_t hash = UINT64_C(1469598103934665603);
  for (size_t i = 0; i < value.size(); ++i) {
    hash ^= static_cast<unsigned char>(value.data()[i]);
    hash *= UINT64_C(1099511628211);
  }
  return Mix(hash);
}

void WriteCsvString(std::ostream& output, const rocksdb::Slice& value) {
  output.put('"');
  for (size_t i = 0; i < value.size(); ++i) {
    char current = value.data()[i];
    if (current == '"') {
      output.put('"');
    }
    output.put(current);
  }
  output.put('"');
}

class CsvBlockCacheTraceWriter final : public rocksdb::BlockCacheTraceWriter {
 public:
  CsvBlockCacheTraceWriter(const std::string& path, uint64_t sample_one_in,
                           uint64_t max_bytes)
      : output_(path, std::ios::out | std::ios::trunc),
        sample_one_in_(sample_one_in),
        max_bytes_(max_bytes) {}

  bool IsOpen() const { return output_.is_open(); }

  rocksdb::Status WriteHeader() override {
    std::lock_guard<std::mutex> lock(mutex_);
    output_ << "timestamp_us,get_id,level,sst_file,caller,block_type,cache_hit,"
               "no_insert,key_exists,block_size,cf_id,cf_name\n";
    output_.flush();
    return rocksdb::Status();
  }

  rocksdb::Status WriteBlockAccess(const rocksdb::BlockCacheTraceRecord& record,
                                   const rocksdb::Slice& block_key,
                                   const rocksdb::Slice& cf_name,
                                   const rocksdb::Slice&) override {
    uint64_t sample_key = record.get_id == kReservedGetId
        ? HashSlice(block_key) : Mix(record.get_id);
    if (sample_key % sample_one_in_ != 0) {
      return rocksdb::Status();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (full_) {
      return rocksdb::Status();
    }
    if (static_cast<uint64_t>(output_.tellp()) >= max_bytes_) {
      full_ = true;
      output_.flush();
      return rocksdb::Status();
    }
    output_ << record.access_timestamp << ',' << record.get_id << ',' << record.level << ','
            << record.sst_fd_number << ',' << static_cast<int>(record.caller) << ','
            << static_cast<int>(record.block_type) << ',' << (record.is_cache_hit ? 1 : 0)
            << ',' << (record.no_insert ? 1 : 0) << ','
            << (record.referenced_key_exist_in_block ? 1 : 0) << ',' << record.block_size
            << ',' << record.cf_id << ',';
    WriteCsvString(output_, cf_name);
    output_.put('\n');
    return rocksdb::Status();
  }

 private:
  std::ofstream output_;
  uint64_t sample_one_in_;
  uint64_t max_bytes_;
  bool full_ = false;
  std::mutex mutex_;
};

std::string JStringToString(JNIEnv* env, jstring value) {
  const char* chars = env->GetStringUTFChars(value, nullptr);
  if (chars == nullptr) {
    return {};
  }
  std::string result(chars);
  env->ReleaseStringUTFChars(value, chars);
  return result;
}

jstring Error(JNIEnv* env, const std::string& message) {
  return env->NewStringUTF(message.c_str());
}

}  // namespace

extern "C" JNIEXPORT jstring JNICALL
Java_org_tron_common_storage_rocksdb_RocksDbBlockCacheTrace_startTrace(
    JNIEnv* env, jclass, jlong database_handle, jstring output_path,
    jlong sample_one_in, jlong max_bytes) {
  if (database_handle == 0 || sample_one_in <= 0 || max_bytes <= 0) {
    return Error(env, "invalid block cache trace arguments");
  }
  auto writer = std::make_unique<CsvBlockCacheTraceWriter>(
      JStringToString(env, output_path), static_cast<uint64_t>(sample_one_in),
      static_cast<uint64_t>(max_bytes));
  if (!writer->IsOpen()) {
    return Error(env, "unable to open trace output");
  }
  const RocksDbTraceFunctions& functions = TraceFunctions();
  if (!functions.error.empty()) {
    return Error(env, functions.error);
  }
  rocksdb::BlockCacheTraceOptions options;
  options.sampling_frequency = 1;
  rocksdb::Status status = functions.start(
      reinterpret_cast<void*>(database_handle), options, std::move(writer));
  return status.ok() ? nullptr : Error(env, status.ToString());
}

extern "C" JNIEXPORT jstring JNICALL
Java_org_tron_common_storage_rocksdb_RocksDbBlockCacheTrace_endTrace(
    JNIEnv* env, jclass, jlong database_handle) {
  if (database_handle == 0) {
    return Error(env, "invalid RocksDB handle");
  }
  const RocksDbTraceFunctions& functions = TraceFunctions();
  if (!functions.error.empty()) {
    return Error(env, functions.error);
  }
  rocksdb::Status status = functions.end(reinterpret_cast<void*>(database_handle));
  return status.ok() ? nullptr : Error(env, status.ToString());
}
