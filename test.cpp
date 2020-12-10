#include <functional>

#include "database.h"

int main() {
    Database db("", true);

    db.performTransaction([](ReadFn readKey, WriteFn writeKey) {
        writeKey("K", "V");
        return true;
    });
}