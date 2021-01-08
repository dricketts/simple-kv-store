#ifndef NDEBUG
#   define ASSERT_EQ(v1, v2) \
    do { \
        if (v1 != v2) { \
            std::cerr << "Equality assertion failed in " << __FILE__ << " line " << __LINE__ << ": " \
                      << #v1 << " (" << v1 << ")" << " != " \
                      << #v2 << " (" << v2 << ")" << std::endl; \
            std::terminate(); \
        } \
    } while (false)
#else
#   define ASSERT_EQ(v1, v2) do { } while (false)
#endif