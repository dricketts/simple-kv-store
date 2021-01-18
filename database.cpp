#include "database.h"
#include <cstring>
#include <atomic>
#include <algorithm>
#include <future>
#include <iostream>
#include <mutex>

#include "util.h"
#include "serdes.h"
#include "exceptions.h"

// TODO: make all of these magic numbers configurable.

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

Database::Database(const std::string& fileName, bool doFormat) :
    logFile_(fileName, FILE_SIZE),
    slotRWMutexes_(NUM_LOG_SLOT)
{
    if (doFormat) format();
    replay();
    
    resetPending();

    cancelFuture_ = cancelPromise_.get_future();

    std::promise<void> commitPromise;
    commitFuture_ = commitPromise.get_future();
    commitThread_ = std::thread(&Database::commitLoop, this, std::move(commitPromise));
}

Database::~Database() {
    cancelPromise_.set_value();
    commitThread_.join();
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
        updateSlotAndExec(slot, [](){});
        char* kvsp = ls->kvs;
        for (int i = 0; i < ls->numKvs; ++i) {
            auto [k, _, newKvsp] = getKV(kvsp);
            index[k] = {slot, ls->kvs - kvsp};
            kvsp = newKvsp;
        }
    }

    latestIndex_ = std::make_shared<Index>(index);
}

void Database::persist() {
    logFile_.persist();
}

struct Database::TransactionMD {
    std::shared_ptr<Index> readIndex;
    std::map<Key, Value> writes;
    size_t writeSize = 0;
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
        write(key, value, txnMD);
    };
    bool wantsCommit = txn(txnRead, txnWrite);
    return tryCommit(wantsCommit, txnMD);
}

// TODO: concurrency control for log trimming
Database::TransactionMD Database::beginTransaction() {
    return {.readIndex = getIndex()};
}

const std::optional<Value> Database::read(const Key& key, TransactionMD& txnMD) {
    txnMD.readSet.insert(key);

    // Try to read from local writes
    if (auto it = txnMD.writes.find(key); it != txnMD.writes.end()) {
        return it->second;
    }

    // Read from the index
    auto it = txnMD.readIndex.get()->find(key);
    if (it == txnMD.readIndex.get()->end()) return {};
    auto [slot, offset] = it->second;
    char* kvp = getLogSlot(slot)->kvs + offset;
    Key k; Value v;
    bool success = checkSlotAndExec(slot, [this, &k, &v, kvp](){
        auto [kk, vv, _] = getKV(kvp);
        k = std::move(kk);
        v = std::move(vv);
    });
    if (success) {
        ASSERT_EQ(k, key);
        return v;
    } else {
        throw StaleRead(slot);
    }
}

void Database::write(const Key& key, const Value& value, TransactionMD& txnMD) const {
    // TODO: does move make any sense here?
    txnMD.writes[key] = std::move(value);
    txnMD.writeSize += kvSerializedSize(key, value);
}

TransactionResult Database::tryCommit(bool wantsCommit, const TransactionMD& txnMD) {
    //   1. Acquire commit mutex
    //   2. Wait for space in pending log slot
    //   3. check for conflicts
    //   4. append to pending log slot
    //   5. unlock commit mutex
    //   6. wait for commit thread to persist pending log slot and
    //      corresponding header
    std::unique_lock lock(commitMutex_);
    commitCond_.wait(lock, [this, &txnMD] {
        return txnMD.writeSize <= pendingSpace();
    });

    if (!checkConflicts(txnMD)) return TransactionResult::Conflict;

    auto commitFuture = append(txnMD.writes);
    lock.unlock();

    commitFuture.wait();

    return TransactionResult::Success;
}

bool Database::checkConflicts(const TransactionMD& txnMD) const {
    return true;
}

// Precondition: thread holds commitMutex
std::shared_future<void> Database::append(const std::map<Key, Value>& kvs) {
    updateSlotAndExec(pendingLogP_.slot, [this, &kvs](){
        LogSlot* ls = getLogSlot(pendingLogP_.slot);
        ls->numKvs += kvs.size();
        for (auto& [k, v] : kvs) {
            pendingIndex_[k] = pendingLogP_;
            char* newOff = memcpyKV(ls->kvs + pendingLogP_.offset, k, v);
            pendingLogP_.offset = newOff - ls->kvs;
        } 
    });

    return commitFuture_;
}

size_t Database::pendingSpace() {
    return LOG_SLOT_PAYLOAD_SIZE - pendingLogP_.offset;
}

// This function pipelines calls to persist() so that there is amortized one
// call per log slot. A single call to persist() is used to persist the values
// written to log slot N and the header pointing to N - 1. Effectively, a
// committing transaction enters a two stage pipeline: stage 1 persists the log
// slot and stage 2 persists the header pointing to that log slot.
//
// The input promise corresponds to commitFuture.
void Database::commitLoop(std::promise<void> commitPromise) {
    LogHeader* lh = getLogHeader();
    
    // There will never be any references to the future for the initial value of
    // stage2Promise. This is because stage2Promise is for transactions in stage
    // 2 of the pipeline, and there are initially no no transactions in stage 2.
    std::promise<void> stage2Promise;
    // stage1Promise is for transactions in stage 1 of the commit pipeline.
    std::promise<void> stage1Promise = std::move(commitPromise);

    while (cancelFuture_.wait_for(std::chrono::milliseconds(10)) ==
            std::future_status::timeout)
    {
        std::scoped_lock lock(commitMutex_);
        persist();
        // Notify transactions that just completed stage 2.
        stage2Promise.set_value();
        stage2Promise = std::move(stage1Promise);
        stage1Promise = std::promise<void>();
        commitFuture_ = stage1Promise.get_future();

        updateIndex(pendingIndex_);
        *lh = pendingHeader_;
        resetPending();
        commitCond_.notify_one();

    }
}

void Database::resetPending() {
    pendingIndex_ = *getIndex();
    LogHeader* lh = getLogHeader();
    pendingHeader_ = *lh;
    pendingHeader_.head++;
    pendingLogP_ = {lh->head, 0};
    getLogSlot(lh->head)->numKvs = 0;
}

void Database::updateSlotAndExec(long slot, std::function<void ()> exec) {
    SlotRWMutex& srw = slotRWMutexes_[getPhysicalLogSlot(slot)];
    std::scoped_lock lock(srw.mutex);
    srw.slot = slot;
    exec();
}

bool Database::checkSlotAndExec(long slot, std::function<void ()> exec) {
    SlotRWMutex& srw = slotRWMutexes_[getPhysicalLogSlot(slot)];
    // TODO: try_to_lock?
    // no point on blocking if the lock is held in exclusive mode
    // but, try_to_lock might introduce spurious failures
    // on the other hand, spurious failures are good for testing
    std::shared_lock lock(srw.mutex);
    if (srw.slot == slot) {
        exec();
        return true;
    } else {
        return false;
    }
}

LogHeader* Database::getLogHeader() const {
    return reinterpret_cast<LogHeader*>(logFile_.getBasePointer());
}

long Database::getPhysicalLogSlot(long n) const {
    return n % NUM_LOG_SLOT;
}

LogSlot* Database::getLogSlot(long n) const {
    return reinterpret_cast<LogSlot*>(logFile_.getBasePointer() + LOG_HEADER_SIZE) + getPhysicalLogSlot(n);
}

std::shared_ptr<Database::Index> Database::getIndex() {
    return std::atomic_load(&latestIndex_);
}

void Database::updateIndex(const Index& newIndex) {
    std::atomic_store(&latestIndex_, std::make_shared<Index>(newIndex));
}