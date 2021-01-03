#include <functional>
#include <iostream>
#include <string>

#include "database.h"

int main() {
    {
        Database db("test_db.db", true);

        for (int i = 0; i < 1024; ++i) {
            db.performTransaction([i](ReadFn readKey, WriteFn writeKey) {
                auto v = readKey("K");
                std::cout << v.value_or("None") << std::endl;
                writeKey("K", std::to_string(i));
                return true;
            });
        }
    }

    std::cout << "==================" << std::endl;

    {
        Database db2("test_db.db", false);
        for (int i = 0; i < 3; ++i) {
            db2.performTransaction([i](ReadFn readKey, WriteFn writeKey) {
                auto v = readKey("K");
                std::cout << v.value_or("None") << std::endl;
                int newVal = std::stoi(v.value_or("0")) + 1;
                writeKey("K", std::to_string(newVal));
                return true;
            });
        }
    }

}