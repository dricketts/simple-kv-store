#include "database.h"
#include <cstring>
#include <atomic>
#include <algorithm>
#include <future>
#include <iostream>
#include <mutex>

#include "util.h"

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
// Number of consecutive log slots required to replay the state machine
static const long NUM_LOG_SLOT_REPLAY = 128;
// One extra log slot because log slot writes are not necessarily atomic.
static const long NUM_LOG_SLOT = NUM_LOG_SLOT_REPLAY + 1;
static const long FILE_SIZE = LOG_HEADER_SIZE + NUM_LOG_SLOT * LOG_SLOT_SIZE;

using Log = LogSlot[NUM_LOG_SLOT];

Database::Database(const std::string& fileName, bool doFormat) : logFile(fileName, FILE_SIZE) {
    if (doFormat) format();
    replay();
    
    resetPending();

    cancelFuture = cancelPromise.get_future();

    std::promise<void> commitPromise;
    commitFuture = commitPromise.get_future();
    commitThread = std::thread(&Database::commitLoop, this, std::move(commitPromise));
}

Database::~Database() {
    cancelPromise.set_value();
    commitThread.join();
}

void Database::format() {
    LogHeader* lh = getLogHeader();
    lh->head = 0;
    persist();
}

void Database::replay() {
    Index index;
    const LogHeader* lh = getLogHeader();
    for (long slot = std::max(0L, lh->head - NUM_LOG_SLOT_REPLAY); slot < lh->head; ++slot) {
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

void Database::persist() {
    logFile.persist();
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
    ASSERT_EQ(k, key);
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
    std::unique_lock lock(commitMutex);

    // Step 1
    if (!checkConflicts(txnMD)) return TransactionResult::Conflict;

    // Step 2-6
    auto commitFuture = append(txnMD.writes);
    lock.unlock();

    commitFuture.wait();

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

// Precondition: thread holds commitMutex
std::shared_future<void> Database::append(const std::map<Key, Value>& kvs) {
    
    pendingLogSlot->numKvs += kvs.size();
    for (auto& [k, v] : kvs) {
        pendingIndex[k] = pendingKvs;
        pendingKvs = memcpyString(pendingKvs, k);
        pendingKvs = memcpyString(pendingKvs, v);
    }

    return commitFuture;
}

void Database::commitLoop(std::promise<void> commitPromise) {
    LogHeader* lh = getLogHeader();
    
    std::promise<void> currentCommitPromise;
    std::promise<void> nextCommitPromise = std::move(commitPromise);

    while (cancelFuture.wait_for(std::chrono::milliseconds(10)) ==
            std::future_status::timeout) {
        std::scoped_lock lock(commitMutex);
        persist();
        currentCommitPromise.set_value();
        currentCommitPromise = std::move(nextCommitPromise);
        nextCommitPromise = std::promise<void>();
        commitFuture = nextCommitPromise.get_future();

        updateIndex(pendingIndex);
        *lh = pendingHeader;
        resetPending();

    }
}

void Database::resetPending() {
    pendingIndex = *getIndex();
    LogHeader* lh = getLogHeader();
    pendingHeader = *lh;
    pendingHeader.head++;
    pendingLogSlot = getLogSlot(lh->head);
    pendingLogSlot->numKvs = 0;
    pendingKvs = pendingLogSlot->kvs;
}

LogHeader* Database::getLogHeader() const {
    return reinterpret_cast<LogHeader*>(logFile.getBasePointer());
}

LogSlot* Database::getLogSlot(long n) const {
    return reinterpret_cast<LogSlot*>(logFile.getBasePointer() + LOG_HEADER_SIZE) + (n % NUM_LOG_SLOT);
}

std::shared_ptr<Database::Index> Database::getIndex() {
    return std::atomic_load(&latestIndex);
}

void Database::updateIndex(const Index& newIndex) {
    std::atomic_store(&latestIndex, std::make_shared<Index>(newIndex));
}