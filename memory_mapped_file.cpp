#include "memory_mapped_file.h"

#include <iostream>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

MemoryMappedFile::MemoryMappedFile(const std::string& fileName, long fileSize)
    : fileSize{fileSize}
{
    fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1)
        handle_error("open");

    // obtain file size
    struct stat sb;
    if (fstat(fd, &sb) == -1)
        handle_error("fstat");

    // long curFileSize = sb.st_size;
    // if (curFileSize != fileSize)
    //     handle_error("file size");

    if (ftruncate(fd, fileSize) == -1) {
        handle_error("truncate");
    };
    
    fileMemory = static_cast<char*>(mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0u));
    if (fileMemory == MAP_FAILED)
        handle_error("mmap");
}

MemoryMappedFile::~MemoryMappedFile() {
    munmap(fileMemory, fileSize);
    close(fd);
}

void MemoryMappedFile::handle_error(const char* msg) {
    perror(msg); 
    exit(255);
}

char* MemoryMappedFile::getBasePointer() const {
    return fileMemory;
}

void MemoryMappedFile::persist() {
    fsync(fd);
}