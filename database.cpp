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

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
Database(const std::string& fileName, bool doFormat) :
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

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
~Database() {
    cancelPromise_.set_value();
    commitThread_.join();
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
format() {
    LogHeader* lh = getLogHeader();
    lh->head = 0;
    persist();
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
replay() {
    Index index;
    const LogHeader* lh = getLogHeader();
    for (long slot = std::max(0L, lh->head - NUM_LOG_SLOT_REPLAY); slot < lh->head; ++slot) {
        updateSlotAndExec(slot, [slot, &index](LogSlot* ls){
            const char* kvsp = ls->kvs;
            for (int i = 0; i < ls->numKvs; ++i) {
                auto [k, _, newKvsp] = getKV(kvsp);
                index[k] = {slot, ls->kvs - kvsp};
                kvsp = newKvsp;
            }
        });
    }

    latestIndex_ = std::make_shared<Index>(index);
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
persist() {
    logFile_.persist();
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
struct Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::TransactionMD {
    std::shared_ptr<Index> readIndex;
    std::map<Key, Value> writes;
    size_t writeSize = 0;
    std::unordered_set<Key> readSet;
};

// TODO: handle too many writes here.
// Also, give read and write some way of failing the transaction.
template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
TransactionResult Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
performTransaction(Transaction txn) {
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
template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
typename Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::TransactionMD
Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
beginTransaction() {
    return {.readIndex = getIndex()};
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
const std::optional<Value> Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
read(const Key& key, TransactionMD& txnMD) {
    txnMD.readSet.insert(key);

    // Try to read from local writes
    if (auto it = txnMD.writes.find(key); it != txnMD.writes.end()) {
        return it->second;
    }

    // Read from the index
    auto it = txnMD.readIndex.get()->find(key);
    if (it == txnMD.readIndex.get()->end()) return {};
    // cannot use structured binding for slot and offset because offset is
    // captured by the lambda below
    long slot = it->second.slot;
    long offset = it->second.offset;
    Key k; Value v;
    checkSlotAndExec(slot, [this, &k, &v, offset](const LogSlot* ls){
        auto [kk, vv, _] = getKV(ls->kvs + offset);
        k = std::move(kk);
        v = std::move(vv);
    });
    ASSERT_EQ(k, key);
    return v;
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
write(const Key& key, const Value& value, TransactionMD& txnMD) const {
    // TODO: does move make any sense here?
    txnMD.writes[key] = std::move(value);
    txnMD.writeSize += kvSerializedSize(key, value);
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
TransactionResult Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
tryCommit(bool wantsCommit, const TransactionMD& txnMD) {
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

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
bool Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
checkConflicts(const TransactionMD& txnMD) const {
    return true;
}

// Precondition: thread holds commitMutex
template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
std::shared_future<void> Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
append(const std::map<Key, Value>& kvs) {
    updateSlotAndExec(pendingLogP_.slot, [this, &kvs](LogSlot* ls){
        ls->numKvs += kvs.size();
        for (auto& [k, v] : kvs) {
            pendingIndex_[k] = pendingLogP_;
            char* newOff = memcpyKV(ls->kvs + pendingLogP_.offset, k, v);
            pendingLogP_.offset = newOff - ls->kvs;
        } 
    });

    return commitFuture_;
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
size_t Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
pendingSpace() {
    return LOG_SLOT_PAYLOAD_SIZE - pendingLogP_.offset;
}

// This function pipelines calls to persist() so that there is amortized one
// call per log slot. A single call to persist() is used to persist the values
// written to log slot N and the header pointing to N - 1. Effectively, a
// committing transaction enters a two stage pipeline: stage 1 persists the log
// slot and stage 2 persists the header pointing to that log slot.
//
// The input promise corresponds to commitFuture.
template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
commitLoop(std::promise<void> commitPromise) {
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

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
resetPending() {
    pendingIndex_ = *getIndex();
    LogHeader* lh = getLogHeader();
    pendingHeader_ = *lh;
    pendingHeader_.head++;
    pendingLogP_ = {lh->head, 0};
    updateSlotAndExec(lh->head, [](LogSlot* ls){
        ls->numKvs = 0;
    });
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
updateSlotAndExec(long slot, std::function<void (LogSlot*)> exec) {
    SlotRWMutex& srw = slotRWMutexes_[getPhysicalLogSlot(slot)];
    std::scoped_lock lock(srw.mutex);
    // TODO: check slot for non-decreasing
    // throw exception or return bool??
    if (srw.slot <= slot) {
        srw.slot = slot;
        exec(getLogSlot(slot));
    } else {
        throw InvalidSlotWrite(slot);
    }
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
checkSlotAndExec(long slot, std::function<void (const LogSlot*)> exec) {
    SlotRWMutex& srw = slotRWMutexes_[getPhysicalLogSlot(slot)];
    // TODO: try_to_lock?
    // no point on blocking if the lock is held in exclusive mode
    // but, try_to_lock might introduce spurious failures
    // on the other hand, spurious failures are good for testing
    std::shared_lock lock(srw.mutex);
    if (srw.slot == slot) {
        const LogSlot* ls = getLogSlot(slot);
        exec(ls);
    } else {
        throw StaleSlotRead(slot);
    }
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
LogHeader* Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
getLogHeader() const {
    return reinterpret_cast<LogHeader*>(logFile_.getBasePointer());
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
long Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
getPhysicalLogSlot(long n) const {
    return n % NUM_LOG_SLOT;
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
typename Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::LogSlot* Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
getLogSlot(long n) const {
    return reinterpret_cast<LogSlot*>(logFile_.getBasePointer() + LOG_HEADER_SIZE) + getPhysicalLogSlot(n);
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
std::shared_ptr<typename Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::Index>
Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
getIndex() {
    return std::atomic_load(&latestIndex_);
}

template <long LOG_SLOT_PAYLOAD_SIZE, long NUM_LOG_SLOT_REPLAY>
void Database<LOG_SLOT_PAYLOAD_SIZE, NUM_LOG_SLOT_REPLAY>::
updateIndex(const Index& newIndex) {
    std::atomic_store(&latestIndex_, std::make_shared<Index>(newIndex));
}