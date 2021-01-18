#include <stdexcept>

class StaleSlotRead : public std::runtime_error {
public:
    StaleSlotRead(long slot) : std::runtime_error("Stale read on slot " + std::to_string(slot)) { }
};

class InvalidSlotWrite : public std::runtime_error {
public:
    InvalidSlotWrite(long slot) : std::runtime_error("Invalid write to slot " + std::to_string(slot)) { }
};
