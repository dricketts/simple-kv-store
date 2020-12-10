#include <string>
#include <map>
#include <unordered_set>
#include <functional>

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
using ReadFn = std::function<const Value* (const Key&)>;
using WriteFn = std::function<void (const Key& key, const Value& value)>;
using Transaction = std::function<bool (ReadFn, WriteFn)>;

// TODO: is there a way to make this private while still defining
// LOG_HEADER_SIZE in the CPP file?
struct LogHeader;

// TODO: add more information to transaction result
enum class TransactionResult {
    Success,
    Conflict,
    Other,
};

class Database {
public:
    Database(const std::string& fileName, bool doFormat);
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

    // TODO: get the root, reserve stuff, do beginTxn stuff
    TransactionMD beginTransaction();
    // TODO: is there a way to return a reference to somewhere in fileMemory, instead of a pointer?
    const Value* read(const Key& key, TransactionMD& root) const;
    void write(const Key& key, const Value& value, std::map<Key, Value>& writes) const;
    TransactionResult tryCommit(bool wantsCommit, const TransactionMD& txnMD);
    bool checkConflicts(const TransactionMD& txnMD) const;

    // TODO: should the return a pointer or a reference?
    LogHeader* getLogHeader();

    // TODO: error codes/exceptions??
    void open(const std::string& fileName);
    void format();
    void close();
    void persist();

    void* fileMemory;
    long fileSize;
};