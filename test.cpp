#include <functional>
#include <iostream>
#include <optional>
#include <string>

#include "database.h"
#include "util.h"

static std::optional<Value> readAndExpect(ReadFn readKey, const Key& key,
                          const std::optional<Value>& expected) {
    auto v = readKey(key);
    ASSERT_EQ(*v, *expected);
    return v;
}

static void test1() {
    std::cout << "Starting test 1" << std::endl;
    int maxRound1 = 248;
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
        Database db("test_db.db", false);
        for (int i = 1; i < 10; ++i) {
            db.performTransaction([i, maxRound1](ReadFn readKey, WriteFn writeKey) {
                auto v = readAndExpect(readKey, "K", std::to_string(maxRound1 + i - 1));
                int newVal = std::stoi(v.value_or("0")) + 1;
                writeKey("K", std::to_string(newVal));
                readAndExpect(readKey, "K", std::to_string(newVal));
                return true;
            });
        }
    }
}

static void test2() {
    std::cout << "Starting test 2" << std::endl;
    Database db("test_db.db", true);
    std::vector<std::thread> threads;
    for (int i = 0; i < 1024; ++i) {
        threads.push_back(
            std::thread([&db](){
                db.performTransaction([](ReadFn readKey, WriteFn writeKey) {
                    writeKey("K", "V");
                    return true;
                });
            })
        );
    }

    for (auto& t : threads) {
        t.join();
    }
}

static void test3() {
    std::cout << "Starting test 3" << std::endl;
    Database db("test_db.db", true);
    std::vector<std::thread> threads;
    for (int i = 0; i < 1024; ++i) {
        threads.push_back(
            std::thread([&db, i](){
                for (int j = 0; j < 128; ++j) {
                    db.performTransaction([i, j](ReadFn readKey, WriteFn writeKey) {
                        auto v =  i == 0 ? readKey(std::to_string(i) + "0") : "Blah";
                        writeKey(std::to_string(i) + std::to_string(j), v.value_or("None"));
                        return true;
                    });
                }
            })
        );
    }

    for (auto& t : threads) {
        t.join();
    }
}

int main() {
    test1();
    test2();
    test3();
}