struct Request {
    uint64_t timestamp;
    int type; // read/write
    uint64_t key;
    uint64_t value;
};

