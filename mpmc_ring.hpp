// mpmc_ring.hpp
// g++ -std=gnu++17 -O2 -pthread demo.cpp -o demo
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <memory>

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

template <typename T>
class MPMCRing {
  static_assert(std::is_move_constructible<T>::value,
                "T must be move-constructible");

  struct alignas(CACHELINE_SIZE) Cell {
    std::atomic<size_t> seq;
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
  };

  alignas(CACHELINE_SIZE) std::unique_ptr<Cell[]> buf_;
  size_t cap_;
  size_t mask_; // cap_ - 1 (power-of-two)
  alignas(CACHELINE_SIZE) std::atomic<size_t> enq_{0};
  alignas(CACHELINE_SIZE) std::atomic<size_t> deq_{0};

  static size_t next_pow2(size_t x) {
    if (x < 2) return 2;
    --x;
    for (size_t i = 1; i < sizeof(size_t) * 8; i <<= 1) x |= x >> i;
    return x + 1;
  }

  static T* ptr(void* p) noexcept {
    return reinterpret_cast<T*>(p);
  }

public:
  explicit MPMCRing(size_t capacity) {
    cap_  = next_pow2(capacity);
    mask_ = cap_ - 1;
    buf_.reset(new Cell[cap_]);
    for (size_t i = 0; i < cap_; ++i) {
      buf_[i].seq.store(i, std::memory_order_relaxed);
    }
  }

  ~MPMCRing() {
    // 남은 요소 안전 파괴 시도(강제 drain 아님)
    T tmp;
    while (try_pop(tmp)) {}
  }

  MPMCRing(const MPMCRing&) = delete;
  MPMCRing& operator=(const MPMCRing&) = delete;

  // 실패 시 false (가득 참)
  bool try_push(const T& v) { return emplace(v); }
  bool try_push(T&& v)      { return emplace(std::move(v)); }

  template <class... Args>
  bool emplace(Args&&... args) {
    size_t pos = enq_.load(std::memory_order_relaxed);
    for (;;) {
      Cell* cell = &buf_[pos & mask_];
      size_t seq = cell->seq.load(std::memory_order_acquire);
      intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (dif == 0) {
        if (enq_.compare_exchange_weak(pos, pos + 1,
                                       std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
          ::new (&cell->storage) T(std::forward<Args>(args)...);
          cell->seq.store(pos + 1, std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        // full
        return false;
      } else {
        pos = enq_.load(std::memory_order_relaxed);
      }
    }
  }

  // 실패 시 false (비어 있음)
  bool try_pop(T& out) {
    size_t pos = deq_.load(std::memory_order_relaxed);
    for (;;) {
      Cell* cell = &buf_[pos & mask_];
      size_t seq = cell->seq.load(std::memory_order_acquire);
      intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
      if (dif == 0) {
        if (deq_.compare_exchange_weak(pos, pos + 1,
                                       std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
          T* p = ptr(&cell->storage);
          out = std::move(*p);
          p->~T();
          // 다음 생산자를 위해 sequence를 cap 만큼 점프
          cell->seq.store(pos + cap_, std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        // empty
        return false;
      } else {
        pos = deq_.load(std::memory_order_relaxed);
      }
    }
  }

  // 대략적 사이즈 (정확성 보장 X)
  size_t size_approx() const {
    size_t e = enq_.load(std::memory_order_relaxed);
    size_t d = deq_.load(std::memory_order_relaxed);
    return e - d;
  }

  size_t capacity() const { return cap_; }
};

