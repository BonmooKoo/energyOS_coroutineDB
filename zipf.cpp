#include "zipf.hpp"
#include <cmath>
#include <vector>
#include <algorithm>
#include <random>

ZipfGenerator::ZipfGenerator(size_t n, double s)
    : N(n), skew(s), dist(n), gen(std::random_device{}()), dis(0.0, 1.0) {
    double sum = 0.0;
    for (size_t i = 1; i <= N; ++i) {
        sum += 1.0 / std::pow(i, skew);
    }
    double c = 1.0 / sum;
    for (size_t i = 0; i < N; ++i) {
        dist[i] = c / std::pow(i + 1, skew);
        if (i > 0) dist[i] += dist[i - 1];
    }
}

size_t ZipfGenerator::Next() {
    double rnd = dis(gen);
    return std::lower_bound(dist.begin(), dist.end(), rnd) - dist.begin();
}

