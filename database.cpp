#include "database.h"

struct LogHeader {
    // TODO: add CRC

    // Pointers relative to the start of the log portion of fileMemory
    // The log slots are all the bits between tail and head.
    long tail;
    long head;
};

struct LogSlot {
    // TODO: add CRC

};

static const long LOG_HEADER_SIZE = sizeof(LogHeader);
// Log header needs to be written atomically, so it cannot exceed the size
// of the atomic write unit of the storage medium.
// TODO: there's probably some alignment stuff to worry about as well.
static_assert(LOG_HEADER_SIZE <= 512, "Log header size exceeds atomic write unit size.");
static const long LOG_SLOT_SIZE = sizeof(LogSlot);
static const long NUM_LOG_SLOT = 128;
static const long FILE_SIZE = LOG_HEADER_SIZE + NUM_LOG_SLOT * LOG_SLOT_SIZE;

Database::Database(const std::string& fileName, bool doFormat) {
    open(fileName);
    if (doFormat) format();
}

Database::~Database() {
    close();
}

void Database::format() {
    LogHeader* lh = getLogHeader();
    lh->head = 0;
    lh->tail = 0;
    persist();
}

// TODO: open, close, and persist be implemented against a file
void Database::open(const std::string& fileName) {
    fileMemory = ::operator new(FILE_SIZE);
    fileSize = FILE_SIZE;
}

void Database::close() {
    ::operator delete(fileMemory);
}

void Database::persist() {

}

struct Database::TransactionMD {
    std::map<Key, Value> writes;
    std::unordered_set<Key> readSet;
};

TransactionResult Database::performTransaction(Transaction txn) {
    Database::TransactionMD txnMD = beginTransaction();
    auto txnRead = [this, &txnMD](const Key& key) -> const Value* {
        return read(key, txnMD);
    };
    auto txnWrite = [this, &txnMD](const Key& key, const Value& value) {
        write(key, value, txnMD.writes);
    };
    bool wantsCommit = txn(txnRead, txnWrite);
    return tryCommit(wantsCommit, txnMD);
}

// TODO: implement
Database::TransactionMD Database::beginTransaction() {
    return {};
}

// TODO: implement
const Value* Database::read(const Key& key, TransactionMD& root) const {
    return nullptr;
}

void Database::write(const Key& key, const Value& value, std::map<Key, Value>& writes) const {
    // TODO: does move make any sense here?
    writes[key] = std::move(value);
}

TransactionResult Database::tryCommit(bool wantsCommit, const TransactionMD& txnMD) {
    // For simplicitly do all of this in a critical section.
    //   1. conflict checking - just look at the latest tree or whatever data structure
    //   2. Write data up to new root
    //   3. Fsync
    //   4. Update next meta page
    //   5. Fsync (possibly piggyback this on fsync for next transaction)
    // There might be ways to increase concurrency here.
    return TransactionResult::Success;
}

bool Database::checkConflicts(const TransactionMD& txnMD) const {
    return true;
}

LogHeader* Database::getLogHeader() {
    // TODO: is this a good idea?
    return reinterpret_cast<LogHeader*>(fileMemory);
}