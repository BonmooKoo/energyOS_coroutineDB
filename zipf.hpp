#ifndef ZIPF_HPP
#define ZIPF_HPP

#include <random>

class ZipfGenerator {
public:
    ZipfGenerator(size_t n, double s);
    size_t Next();

private:
    size_t N;
    double skew;
    std::vector<double> dist;
    std::mt19937_64 gen;
    std::uniform_real_distribution<> dis;
};

#endif // ZIPF_HPP
