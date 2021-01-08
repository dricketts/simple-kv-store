#include <cstring>

#include "util.h"

static size_t stringSerializedSize(const std::string& str) {
    return sizeof(size_t) + str.size();
}

static size_t kvSerializedSize(const std::string& key, const std::string& value) {
    return stringSerializedSize(key) + stringSerializedSize(value);
}

static char* memcpyString(char* dest, const std::string& str) {
    size_t sz = str.size();
    char* end = dest;
    std::memcpy(end, &sz, sizeof(size_t));
    end += sizeof(size_t);
    std::memcpy(end, str.c_str(), sz);
    end += sz;
    ASSERT_EQ(end, dest + stringSerializedSize(str));
    return end;
}

static char* memcpyKV(char* dest, const std::string& key, const std::string& value) {
    char* end = dest;
    end = memcpyString(end, key);
    end = memcpyString(end, value);
    ASSERT_EQ(end, dest + kvSerializedSize(key, value));
    return end;
}

static std::tuple<std::string, std::string, char*> getKV(char* src) {
    size_t ksize = *src;
    char* kp = src + sizeof(size_t);
    const std::string key {kp, ksize};
    size_t vsize = *(kp + ksize);
    char* vp = kp + ksize + sizeof(size_t);
    const std::string value {vp, vsize};
    char* end = vp + vsize;
    ASSERT_EQ(end, src + kvSerializedSize(key, value));
    return {key, value, end};
}