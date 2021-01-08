#include <string>

/*
 * Represents a memory mapped file. Constructor performs all creation and
 * error handling. Destructor cleans up file resources. The function
 * getBasePointer() gives a pointer to the start of the file.
 *
 * This currently only works on Linux.
 */
class MemoryMappedFile {
public:
    /*
     * Creates a file of the given name and size if it doesn't exist. Throws an
     * exception if the file exists and is of the wrong size. Opens and memory
     * maps the file.
     */
    MemoryMappedFile(const std::string& fileName, long fileSize);
    /*
     * Cleans up all resources associated with the file.
     */
    ~MemoryMappedFile();

    /*
     * Returns a pointer to the start of the file.
     */
    char* getBasePointer() const;

    /*
     * Persists file contents to stable storage.
     */
    void persist();

private:
    void handle_error(const char* msg);

    int fd;
    char* fileMemory;
    long fileSize;
};