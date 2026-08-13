#ifndef CROUTINE_INTERNAL_QUEUE_H
#define CROUTINE_INTERNAL_QUEUE_H

#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>

// Means: A SEQ64_XX B
#define SEQ64_EQ(A, B) ((A) == (B))
#define SEQ64_BEFORE(A, B) ((int64_t)((uint64_t)(A) - (uint64_t)(B)) < 0)
#define SEQ64_AFTER(A, B) SEQ64_BEFORE(B, A)
#define SEQ64_BEFORE_EQ(A, B) (!SEQ64_AFTER(A, B))
#define SEQ64_AFTER_EQ(A, B) (!SEQ64_BEFORE(A, B))

/*
 * Bounded MPMC pointer ring queue.
 */

typedef struct croutine_mpmc_queue_item {
	_Atomic uint64_t sequence;
	void *item;
} croutine_mpmc_queue_item;

typedef struct croutine_mpmc_queue {
	alignas(64) _Atomic uint64_t head;
	alignas(64) _Atomic uint64_t tail;
	alignas(64) size_t capacity;
	struct croutine_mpmc_queue_item *items;
} croutine_mpmc_queue;

static inline struct croutine_mpmc_queue_item *croutine_mpmc_queue_at(struct croutine_mpmc_queue *queue, uint64_t pos) {
	return queue->items + (pos & (queue->capacity - 1));
}

static inline struct croutine_mpmc_queue *croutine_mpmc_queue_init(size_t capacity) {
	if (capacity < 2 || (capacity & (capacity - 1)) != 0 || capacity > (UINT64_MAX >> 1) ||
		capacity > (SIZE_MAX / sizeof(croutine_mpmc_queue_item)))
		return NULL;

	struct croutine_mpmc_queue_item *cells = malloc(sizeof(croutine_mpmc_queue_item) * capacity);
	if (!cells)
		return NULL;

	for (size_t i = 0; i < capacity; i++) {
		cells[i].item = NULL;
		atomic_init(&cells[i].sequence, i);
	}

	struct croutine_mpmc_queue *queue = malloc(sizeof(croutine_mpmc_queue));
	if (!queue) {
		free(cells);
		return NULL;
	}

	queue->capacity = capacity;
	queue->items = cells;
	atomic_init(&queue->head, 0);
	atomic_init(&queue->tail, 0);

	return queue;
}

static inline void croutine_mpmc_queue_destroy(struct croutine_mpmc_queue *queue) {
	if (queue == NULL)
		return;

	free(queue->items);
	free(queue);
}

static inline int croutine_mpmc_queue_push(struct croutine_mpmc_queue *queue, void *T) {
	if (queue == NULL || T == NULL)
		return -1;

	uint64_t pos;
	struct croutine_mpmc_queue_item *item = NULL;

	for (;;) {
		pos = atomic_load_explicit(&queue->head, memory_order_relaxed);
		item = croutine_mpmc_queue_at(queue, pos);
		uint64_t seq = atomic_load_explicit(&item->sequence, memory_order_acquire);

		if (SEQ64_EQ(seq, pos)) {
			if (atomic_compare_exchange_weak_explicit(&queue->head, &pos, pos + 1, memory_order_relaxed,
													  memory_order_relaxed)) {
				break;
			}
		} else if (SEQ64_BEFORE(seq, pos)) {
			return -1;
		}
	}

	item->item = T;
	atomic_store_explicit(&item->sequence, pos + 1, memory_order_release);

	return 1;
}

static inline void *croutine_mpmc_queue_pop(struct croutine_mpmc_queue *queue) {
	if (queue == NULL)
		return NULL;

	uint64_t pos;
	struct croutine_mpmc_queue_item *item = NULL;

	for (;;) {
		pos = atomic_load_explicit(&queue->tail, memory_order_relaxed);
		item = croutine_mpmc_queue_at(queue, pos);
		uint64_t seq = atomic_load_explicit(&item->sequence, memory_order_acquire);

		if (SEQ64_EQ(seq, pos + 1)) {
			if (atomic_compare_exchange_weak_explicit(&queue->tail, &pos, pos + 1, memory_order_relaxed,
													  memory_order_relaxed)) {
				break;
			}
		} else if (SEQ64_BEFORE(seq, pos + 1)) {
			return NULL;
		}
	}

	void *res = item->item;
	atomic_store_explicit(&item->sequence, pos + queue->capacity, memory_order_release);

	return res;
}

static inline size_t croutine_mpmc_queue_len(struct croutine_mpmc_queue *queue) {
	if (queue == NULL)
		return 0;

	uint64_t head = atomic_load_explicit(&queue->head, memory_order_relaxed);
	uint64_t tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);

	int64_t diff = (int64_t)(head - tail);
	return diff > 0 ? (size_t)diff : 0;
}

/*
 * Bounded deque.
 *
 * The tail side has a single caller for push/pop. The head side supports
 * concurrent steal callers. Indexes are monotonic modulo-2^64 counters;
 * capacity stays below half the sequence space so comparisons are unambiguous.
 */

typedef struct croutine_cldeque {
	alignas(64) _Atomic uint64_t head;
	alignas(64) _Atomic uint64_t tail;
	alignas(64) size_t capacity;
	_Atomic(void *) *items;
} croutine_cldeque;

static inline _Atomic(void *) *croutine_cldeque_at(struct croutine_cldeque *deque, uint64_t pos) {
	return deque->items + (pos & (uint64_t)(deque->capacity - 1));
}

static inline struct croutine_cldeque *croutine_cldeque_init(size_t capacity) {
	_Atomic(void *) *items;

	if (capacity < 2 || (capacity & (capacity - 1)) != 0 || capacity > (UINT64_MAX >> 1) ||
		capacity > (SIZE_MAX / sizeof(*items)))
		return NULL;

	items = malloc(sizeof(*items) * capacity);
	if (!items)
		return NULL;

	for (size_t i = 0; i < capacity; i++)
		atomic_init(&items[i], NULL);

	struct croutine_cldeque *deque = malloc(sizeof(croutine_cldeque));
	if (!deque) {
		free(items);
		return NULL;
	}

	deque->capacity = capacity;
	deque->items = items;
	atomic_init(&deque->head, 0);
	atomic_init(&deque->tail, 0);

	return deque;
}

static inline void croutine_cldeque_destroy(struct croutine_cldeque *deque) {
	if (deque == NULL)
		return;

	free(deque->items);
	free(deque);
}

static inline int croutine_cldeque_push(struct croutine_cldeque *deque, void *T) {
	if (deque == NULL || T == NULL)
		return -1;

	uint64_t tail = atomic_load_explicit(&deque->tail, memory_order_relaxed);
	uint64_t head = atomic_load_explicit(&deque->head, memory_order_acquire);

	if (SEQ64_AFTER_EQ(tail, head + (uint64_t)deque->capacity))
		return -1;

	atomic_store_explicit(croutine_cldeque_at(deque, tail), T, memory_order_relaxed);
	atomic_store_explicit(&deque->tail, tail + 1, memory_order_release);
	return 1;
}

static inline void *croutine_cldeque_pop(struct croutine_cldeque *deque) {
	if (deque == NULL)
		return NULL;

	uint64_t tail = atomic_load_explicit(&deque->tail, memory_order_relaxed) - 1;
	atomic_store_explicit(&deque->tail, tail, memory_order_relaxed);

	atomic_thread_fence(memory_order_seq_cst);

	uint64_t head = atomic_load_explicit(&deque->head, memory_order_relaxed);

	if (SEQ64_AFTER(head, tail)) {
		atomic_store_explicit(&deque->tail, tail + 1, memory_order_relaxed);
		return NULL;
	}

	void *res = atomic_load_explicit(croutine_cldeque_at(deque, tail), memory_order_relaxed);

	if (SEQ64_EQ(head, tail)) {
		if (!atomic_compare_exchange_strong_explicit(&deque->head, &head, head + 1, memory_order_seq_cst,
													 memory_order_relaxed))
			res = NULL;
		atomic_store_explicit(&deque->tail, tail + 1, memory_order_relaxed);
	}
	return res;
}

static inline void *croutine_cldeque_steal(struct croutine_cldeque *deque) {
	if (deque == NULL)
		return NULL;

	uint64_t head = atomic_load_explicit(&deque->head, memory_order_acquire);
	uint64_t tail = atomic_load_explicit(&deque->tail, memory_order_acquire);

	if (SEQ64_BEFORE_EQ(tail, head))
		return NULL;

	void *res = atomic_load_explicit(croutine_cldeque_at(deque, head), memory_order_relaxed);

	if (!atomic_compare_exchange_strong_explicit(&deque->head, &head, head + 1, memory_order_seq_cst,
												 memory_order_relaxed))
		return NULL;
	return res;
}

static inline size_t croutine_cldeque_len(struct croutine_cldeque *deque) {
	if (deque == NULL)
		return 0;

	uint64_t head = atomic_load_explicit(&deque->head, memory_order_relaxed);
	uint64_t tail = atomic_load_explicit(&deque->tail, memory_order_relaxed);

	int64_t diff = (int64_t)(tail - head);
	return diff > 0 ? (size_t)diff : 0;
}

#undef SEQ64_EQ
#undef SEQ64_BEFORE
#undef SEQ64_AFTER
#undef SEQ64_BEFORE_EQ
#undef SEQ64_AFTER_EQ

#endif
