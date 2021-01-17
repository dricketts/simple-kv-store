#include <string>
#include <map>
#include <unordered_set>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <cassert>
#include <thread>
#include <future>

#include "memory_mapped_file.h"

/*
 * This class implements a transactional key-value store with an extremely
 * simple API:
 *
 *   - The Database constructor creates a database at the given file name and
 *     formats it as a new database if the format flag is set.
 *
 *   - performTransaction takes a Transaction and attempts to execute it. A
 *     Transaction is an std::function that takes two functions and returns a
 *     bool indicating whether or not the transaction should attempt to commit.
 *     The two parameters of the Transaction are a ReadFn and a WriteFn, which
 *     the Transaction can use to read and write keys/values (which are just
 *     std::strings). The database guarantees the all reads for a given
 *     transaction come from a consistent snapshot of the database and that the
 *     set of committed transactions are strictly serializable.
 *
 * The database is implemented as a log written to a memory-mapped file. This
 * log maintains the invariant that, starting from an empty store, replaying N
 * consecutive log entries up to some slot reconstructs the entire store up to
 * that slot. There is no separate checkpoint from which the log gets replayed.
 * Instead, each log entry contains a partial checkpoint of the store state,
 * enough to record key-value pairs that are at risk of falling more than N
 * slots behind. Garbage collection is implicit in that the log is written as a
 * circular buffer, and log entries more than N slots behind the head can be
 * overwritten.
 *
 * Additionally, the database maintains an in-memory index from key to log
 * offset storing the value of the key. This index is implemented as a
 * persistent (immutable) data structure. This means that each transaction
 * creates a new version of the data structure, possibly sharing some memory
 * with other versions. This allows transactions to easily read from a
 * consistent snapshot without worrying about racing with a concurrent writer.
 * Some additional care must be taken to avoid overwriting log entries that are
 * pointed to by versions used by in-flight transactions. In order to prevent
 * this, when a transaction begins, it will record, in some shared place, the
 * oldest log slot on which it may depend (conservatively N slots behind the
 * current log head). The implementation can either block committing
 * transactions or abort old inflight transactions when such a scenario is
 * imminent. The details are TBD.
 */
using Key = std::string;
using Value = std::string;
using ReadFn = std::function<const std::optional<Value> (const Key&)>;
using WriteFn = std::function<void (const Key& key, const Value& value)>;
using Transaction = std::function<bool (ReadFn, WriteFn)>;

// TODO: is there a way to make this private while still defining
// LOG_HEADER_SIZE in the CPP file?
struct LogHeader {
    // TODO: add CRC

    // Next logical log slot to be appended.
    long head;
};
struct LogSlot;

// TODO: add more information to transaction result
enum class TransactionResult {
    Success,
    Conflict,
    Other,
};

class Database {
public:
    /*
     * Performs all initialization of the database and throws exceptions as
     * needed. There is no need to separately initialize or open the database.
     *
     * If doFormat is set, this will potentially create the file and delete any
     * existing data, initializing an empty store.
     */
    Database(const std::string& fileName, bool doFormat);
    /*
     * Performs all necessary cleanup of the database. There is no need to
     * separately close the database.
     */
    ~Database();

    /*
     * Executes txn, performing reads against a consistent snapshot of the
     * database. The transaction attempts to commit only if txn returns true.
     * The set of committed transactions is guaranteed to be strictly
     * serializable.
     */
    TransactionResult performTransaction(Transaction txn);

private:

    struct TransactionMD;
    TransactionMD beginTransaction();
    const std::optional<Value> read(const Key& key, TransactionMD& txnMD) const;
    void write(const Key& key, const Value& value, TransactionMD& txnMD) const;
    TransactionResult tryCommit(bool wantsCommit, const TransactionMD& txnMD);
    bool checkConflicts(const TransactionMD& txnMD) const;

    /*
     * Called if doFormat is set in the constructor.
     */
    void format();
    /*
     * Replay the log. Necessary at startup.
     */
    void replay();
    /*
     * Writes the current contents of the logFile to stable storage.
     */
    void persist();

    // Pointer into the logFile.
    struct LogPointer {
        long slot;
        long offset;
    };
    using Index = std::map<Key, LogPointer>;

    /*
     * Pointer to the fixed-location log header in the logFile.
     */
    LogHeader* getLogHeader() const;
    /*
     * Gets a pointer to the log slot corresponding to the logical log slot n.
     * The logFile holds a circular log, so more than one logical log slot uses
     * the same LogSlot*.
     */
    LogSlot* getLogSlot(long n) const;
    /*
     * Append the key-values pairs to the log. This returns a future to signal
     * when the appending pairs are durable.
     *
     * This function must only be called while holding commitMutex.
     */
    std::shared_future<void> append(const std::map<Key, Value>& kvs);
    /*
     * Run by a background thread to make key-value pairs durable. Sets
     * promises corresponding to futures returned by append().
     */
    void commitLoop(std::promise<void> commitPromise);

    std::shared_ptr<Index> getIndex();
    void updateIndex(const Index& newIndex);

    /*
     * Returns the space remaining in the current pending log slot.
     */
    size_t pendingSpace();
    /*
     * Resets the pending log slot and corresponding state to point to the
     * an unused log slot after the current log head.
     */
    void resetPending();

    // Fixed-size memory mapped file storing log slots and the log header.
    MemoryMappedFile logFile_;

    // Protects access to the tip of the log.
    std::mutex commitMutex_;
    // Used to signal available space in the pending log slot.
    std::condition_variable commitCond_;

    // Latest index into the log. Should only be accessed with getIndex() and
    // updateIndex().
    std::shared_ptr<Index> latestIndex_;

    // State corresponding to the log slot available for writing new key-value
    // pairs. The critical section of commitLoop() maintains the invariant that
    // getLogHeader() points to the previous slot and that commitFuture
    // signals completion of writes going to pendingLogSlot. These structures
    // must only be accessed under commitMutex.
    Index pendingIndex_;
    LogHeader pendingHeader_;
    LogPointer pendingLogP_;

    // Promise/future pair used to signal cancellation to background thread(s).
    std::promise<void> cancelPromise_;
    std::shared_future<void> cancelFuture_;
    
    // Future used to signal persistence of key-value pairs written to
    // pendingLogSlot.
    std::shared_future<void> commitFuture_;
    // Background thread that runs commitLoop().
    std::thread commitThread_;
};