#include <stdexcept>

class StaleRead : public std::runtime_error {
public:
    StaleRead(long slot) : std::runtime_error("Stale read on slot " + std::to_string(slot)) { }
};