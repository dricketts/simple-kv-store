#include <functional>
#include <iostream>
#include <optional>
#include <string>

#include "database.h"

static std::optional<Value> readAndExpect(ReadFn readKey, const Key& key,
                          const std::optional<Value>& expected) {
    auto v = readKey(key);
    assert(v == expected);
    return v;
}

int main() {
    int maxRound1 = 1024;
    {
        Database db("test_db.db", true);

        for (int i = 1; i <= maxRound1; ++i) {
            db.performTransaction([i](ReadFn readKey, WriteFn writeKey) {
                readAndExpect(readKey, "K", i == 1 ? std::optional<Value>{} : std::to_string(i - 1));
                writeKey("K", std::to_string(i));
                readAndExpect(readKey, "K", std::to_string(i));
                return true;
            });
        }
    }

    std::cout << "==================" << std::endl;

    {
        Database db2("test_db.db", false);
        for (int i = 1; i < 10; ++i) {
            db2.performTransaction([i, maxRound1](ReadFn readKey, WriteFn writeKey) {
                auto v = readAndExpect(readKey, "K", std::to_string(maxRound1 + i - 1));
                int newVal = std::stoi(v.value_or("0")) + 1;
                writeKey("K", std::to_string(newVal));
                readAndExpect(readKey, "K", std::to_string(newVal));
                return true;
            });
        }
    }

}