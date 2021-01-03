#include "database.h"
#include <cassert>
#include <cstring>
#include <atomic>
#include <algorithm>
#include <iostream>

// for mmap
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct LogHeader {
    // TODO: add CRC

    // Next logical log slot to be appended.
    long head;
};

static const long INT_SIZE = sizeof(int);
static const long MAX_NUM_KV_PAIRS = 64;
static const long MAX_KEY_SIZE = 1024;
static const long MAX_VAL_SIZE = 4096;
// Log array of kv pairs:
//     length of key
//     key
//     length of value
//     value
static const long LOG_SLOT_PAYLOAD_SIZE = (INT_SIZE + MAX_KEY_SIZE + INT_SIZE + MAX_VAL_SIZE) * MAX_NUM_KV_PAIRS;

struct LogSlot {
    // TODO: crc
    int numKvs;
    char kvs[LOG_SLOT_PAYLOAD_SIZE];
};


static const long LOG_HEADER_SIZE = sizeof(LogHeader);
// Log header needs to be written atomically, so it cannot exceed the size
// of the atomic write unit of the storage medium.
// TODO: there's probably some alignment stuff to worry about as well.
static_assert(LOG_HEADER_SIZE <= 512, "Log header size exceeds atomic write unit size.");
static const long LOG_SLOT_SIZE = sizeof(LogSlot);
static const long NUM_LOG_SLOT = 128;
// One extra log slot because log slot writes are not necessarily atomic.
static const long FILE_SIZE = LOG_HEADER_SIZE + (NUM_LOG_SLOT + 1) * LOG_SLOT_SIZE;

using Log = LogSlot[NUM_LOG_SLOT];

Database::Database(const std::string& fileName, bool doFormat) {
    openFile(fileName, doFormat);
    if (doFormat) format();
    replay();
}

Database::~Database() {
    closeFile();
}

void Database::format() {
    LogHeader* lh = getLogHeader();
    lh->head = 0;
    persist();
}

static void handle_error(const char* msg) {
    perror(msg); 
    exit(255);
}

// TODO: open, close, and persist be implemented against a file
void Database::openFile(const std::string& fileName, bool doFormat) {
    // fileMemory = static_cast<char*>(::operator new(FILE_SIZE));
    // fileSize = FILE_SIZE;
    fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1)
        handle_error("open");

    // obtain file size
    struct stat sb;
    if (fstat(fd, &sb) == -1)
        handle_error("fstat");

    fileSize = sb.st_size;
    if (fileSize != FILE_SIZE && !doFormat)
        handle_error("file size");
    
    if (doFormat) {
        ftruncate(fd, FILE_SIZE);
        fileSize = FILE_SIZE;
    }
    
    fileMemory = static_cast<char*>(mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0u));
    if (fileMemory == MAP_FAILED)
        handle_error("mmap");
}

void Database::replay() {
    Index index;
    const LogHeader* lh = getLogHeader();
    for (long slot = std::max(0L, lh->head - NUM_LOG_SLOT); slot < lh->head; ++slot) {
        LogSlot* ls = getLogSlot(slot);
        LogPointer lp = ls->kvs;
        for (int i = 0; i < ls->numKvs; ++i) {
            auto [k, _, newLp] = getKV(lp);
            index[k] = lp;
            lp = newLp;
        }
    }

    latestIndex = std::make_shared<Index>(index);
}

void Database::closeFile() {
    // ::operator delete(fileMemory);
    munmap(fileMemory, fileSize);
    close(fd);
}

void Database::persist() {
    fsync(fd);
}

struct Database::TransactionMD {
    std::shared_ptr<Index> readIndex;
    std::map<Key, Value> writes;
    std::unordered_set<Key> readSet;
};

// TODO: handle too many writes here.
// Also, give read and write some way of failing the transaction.
TransactionResult Database::performTransaction(Transaction txn) {
    Database::TransactionMD txnMD = beginTransaction();
    auto txnRead = [this, &txnMD](const Key& key) -> const std::optional<Value> {
        return read(key, txnMD);
    };
    auto txnWrite = [this, &txnMD](const Key& key, const Value& value) {
        write(key, value, txnMD.writes);
    };
    bool wantsCommit = txn(txnRead, txnWrite);
    return tryCommit(wantsCommit, txnMD);
}

// TODO: concurrency control for log trimming
Database::TransactionMD Database::beginTransaction() {
    return {.readIndex = getIndex()};
}

std::tuple<Key, Value, Database::LogPointer> Database::getKV(const LogPointer lp) const {
    size_t ksize = *lp;
    char* kp = lp + sizeof(size_t);
    const Key key {kp, ksize};
    size_t vsize = *(kp + ksize);
    char* vp = kp + ksize + sizeof(size_t);
    const Value value {vp, vsize};
    return {key, value, vp + vsize};
}

const std::optional<Value> Database::read(const Key& key, TransactionMD& txnMD) const {
    txnMD.readSet.insert(key);

    // Try to read from local writes
    if (auto it = txnMD.writes.find(key); it != txnMD.writes.end()) {
        return it->second;
    }

    // Read from the index
    auto it = txnMD.readIndex.get()->find(key);
    if (it == txnMD.readIndex.get()->end()) return {};
    auto [k, v, _] = getKV(it->second);
    assert(k == key);
    return v;
}

void Database::write(const Key& key, const Value& value, std::map<Key, Value>& writes) const {
    // TODO: does move make any sense here?
    writes[key] = std::move(value);
}

TransactionResult Database::tryCommit(bool wantsCommit, const TransactionMD& txnMD) {
    // For simplicitly do all of this in a critical section.
    //   1. conflict checking - just look at the latest tree or whatever data structure
    //   2. Append log entry
    //   3. Fsync
    //   4. Update log header
    //   5. Fsync (possibly piggyback this on fsync for next transaction)
    //   6. Update index
    // There might be ways to increase concurrency here.
    std::scoped_lock lock(commitMutex);

    // Step 1
    if (!checkConflicts(txnMD)) return TransactionResult::Conflict;

    // Step 2-6
    std::shared_ptr<Index> index = getIndex();
    // Just make a copy of the entire index for now.
    Index newIndex = *index.get();
    append(txnMD.writes, newIndex);
    updateIndex(newIndex);

    return TransactionResult::Success;
}

bool Database::checkConflicts(const TransactionMD& txnMD) const {
    return true;
}

static char* memcpyString(char* dest, const std::string& str) {
    size_t sz = str.size();
    std::memcpy(dest, &sz, sizeof(size_t));
    dest += sizeof(size_t);
    std::memcpy(dest, str.c_str(), sz);
    dest += sz;
    return dest;
}

void Database::append(const std::map<Key, Value>& kvs, Index& newIndex) {
    LogHeader* lh = getLogHeader();
    
    long slot = lh->head;
    LogSlot* next = getLogSlot(slot);
    
    next->numKvs = kvs.size();
    char* buf = next->kvs;
    for (auto& [k, v] : kvs) {
        newIndex[k] = buf;
        buf = memcpyString(buf, k);
        buf = memcpyString(buf, v);
    }

    persist();

    lh->head++;

    persist();
}

LogHeader* Database::getLogHeader() const {
    return reinterpret_cast<LogHeader*>(fileMemory);
}

LogSlot* Database::getLogSlot(long n) const {
    return reinterpret_cast<LogSlot*>(fileMemory + LOG_HEADER_SIZE) + (n % NUM_LOG_SLOT);
}

std::shared_ptr<Database::Index> Database::getIndex() {
    return std::atomic_load(&latestIndex);
}

void Database::updateIndex(const Index& newIndex) {
    std::atomic_store(&latestIndex, std::make_shared<Index>(newIndex));
}