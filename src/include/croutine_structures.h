#ifndef CROUTINE_INTERNAL_STRUCTURES_H
#define CROUTINE_INTERNAL_STRUCTURES_H

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>

#include "helpers.h"

/*
 * Embedded Reference Counter
 */

typedef struct croutine_refcount {
	_Atomic uint32_t refs;
} croutine_refcount;

typedef void (*croutine_refcount_release_fn)(
	struct croutine_refcount *refcount);

static inline void croutine_refcount_init(struct croutine_refcount *refcount) {
	atomic_init(&refcount->refs, 1);
}

static inline uint32_t
croutine_refcount_read(const struct croutine_refcount *refcount) {
	return atomic_load_explicit(&refcount->refs, memory_order_acquire);
}

static inline int croutine_refcount_get(struct croutine_refcount *refcount) {
	uint32_t refs;

	refs = atomic_load_explicit(&refcount->refs, memory_order_acquire);
	while (refs != 0) {
		if (atomic_compare_exchange_weak_explicit(
				&refcount->refs, &refs, refs + 1, memory_order_acquire,
				memory_order_relaxed))
			return 0;
	}

	return -1;
}

static inline int
croutine_refcount_release(struct croutine_refcount *refcount,
						  croutine_refcount_release_fn release) {
	uint32_t refs;

	refs = atomic_load_explicit(&refcount->refs, memory_order_acquire);
	while (refs != 0) {
		if (!atomic_compare_exchange_weak_explicit(
				&refcount->refs, &refs, refs - 1, memory_order_release,
				memory_order_relaxed))
			continue;

		if (refs != 1)
			return 0;

		atomic_thread_fence(memory_order_acquire);
		if (release != NULL)
			release(refcount);

		return 1;
	}

	return -1;
}

/*
 * CAS Pointer Ring Queue
 */

typedef struct croutine_queue_cell {
	_Atomic uint64_t sequence;
	_Atomic(void *) item;
} croutine_queue_cell;

typedef struct croutine_queue {
	_Atomic uint64_t head;
	_Atomic uint64_t tail;
	uint32_t capacity;
	croutine_queue_cell *cells;
} croutine_queue;

static inline croutine_queue_cell *
croutine_queue_cell_at(const struct croutine_queue *queue, uint64_t index) {
	return &queue->cells[index % queue->capacity];
}

static inline int croutine_queue_init(struct croutine_queue *queue,
									  uint32_t capacity) {
	croutine_queue_cell *cells;

	if (queue == NULL || capacity == 0)
		return -1;

	cells = malloc((size_t)capacity * sizeof(cells[0]));
	if (cells == NULL)
		return -1;

	atomic_init(&queue->head, 0);
	atomic_init(&queue->tail, 0);
	queue->capacity = capacity;
	queue->cells = cells;
	for (uint32_t index = 0; index < capacity; index++) {
		atomic_init(&cells[index].sequence, index);
		atomic_init(&cells[index].item, NULL);
	}

	return 0;
}

static inline void croutine_queue_destroy(struct croutine_queue *queue) {
	if (queue == NULL)
		return;

	free(queue->cells);
	queue->cells = NULL;
	queue->capacity = 0;
	atomic_store_explicit(&queue->head, 0, memory_order_relaxed);
	atomic_store_explicit(&queue->tail, 0, memory_order_relaxed);
}

static inline void croutine_queue_reset(struct croutine_queue *queue) {
	if (queue == NULL || queue->cells == NULL)
		return;

	atomic_store_explicit(&queue->head, 0, memory_order_relaxed);
	atomic_store_explicit(&queue->tail, 0, memory_order_relaxed);
	for (uint32_t index = 0; index < queue->capacity; index++) {
		atomic_store_explicit(&queue->cells[index].sequence, index,
							  memory_order_relaxed);
		atomic_store_explicit(&queue->cells[index].item, NULL,
							  memory_order_relaxed);
	}
}

static inline uint64_t
croutine_queue_len_approx(const struct croutine_queue *queue) {
	uint64_t head;
	uint64_t tail;
	uint64_t len;

	if (queue == NULL || queue->capacity == 0)
		return 0;

	head = atomic_load_explicit(&queue->head, memory_order_acquire);
	tail = atomic_load_explicit(&queue->tail, memory_order_acquire);
	len = tail - head;
	if (len > queue->capacity)
		len = queue->capacity;
	return len;
}

static inline int croutine_queue_empty(const struct croutine_queue *queue) {
	return croutine_queue_len_approx(queue) == 0;
}

static inline int croutine_queue_full(const struct croutine_queue *queue) {
	return queue != NULL && croutine_queue_len_approx(queue) >= queue->capacity;
}

static inline int croutine_queue_push(struct croutine_queue *queue,
									  void *item) {
	croutine_queue_cell *cell;
	uint64_t tail;
	uint64_t sequence;
	int64_t diff;

	if (queue == NULL || queue->capacity == 0)
		return -1;

	tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
	for (;;) {
		cell = croutine_queue_cell_at(queue, tail);
		sequence = atomic_load_explicit(&cell->sequence, memory_order_acquire);
		diff = (int64_t)(sequence - tail);
		if (diff == 0) {
			if (atomic_compare_exchange_weak_explicit(
					&queue->tail, &tail, tail + 1, memory_order_relaxed,
					memory_order_relaxed))
				break;
		} else if (diff < 0) {
			return -1;
		} else {
			tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
		}
	}

	atomic_store_explicit(&cell->item, item, memory_order_relaxed);
	atomic_store_explicit(&cell->sequence, tail + 1, memory_order_release);
	return 0;
}

static inline int croutine_queue_push_owner(struct croutine_queue *queue,
											void *item) {
	croutine_queue_cell *cell;
	uint64_t tail;
	uint64_t sequence;

	if (queue == NULL || queue->capacity == 0)
		return -1;

	tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
	cell = croutine_queue_cell_at(queue, tail);
	sequence = atomic_load_explicit(&cell->sequence, memory_order_acquire);
	if (sequence != tail)
		return -1;

	atomic_store_explicit(&cell->item, item, memory_order_relaxed);
	atomic_store_explicit(&cell->sequence, tail + 1, memory_order_release);
	atomic_store_explicit(&queue->tail, tail + 1, memory_order_release);
	return 0;
}

static inline int croutine_queue_pop(struct croutine_queue *queue,
									 void **item) {
	croutine_queue_cell *cell;
	uint64_t head;
	uint64_t sequence;
	int64_t diff;

	if (queue == NULL || item == NULL || queue->capacity == 0)
		return -1;

	head = atomic_load_explicit(&queue->head, memory_order_relaxed);
	for (;;) {
		cell = croutine_queue_cell_at(queue, head);
		sequence = atomic_load_explicit(&cell->sequence, memory_order_acquire);
		diff = (int64_t)(sequence - (head + 1));
		if (diff == 0) {
			if (atomic_compare_exchange_weak_explicit(
					&queue->head, &head, head + 1, memory_order_acq_rel,
					memory_order_relaxed))
				break;
		} else if (diff < 0) {
			return -1;
		} else {
			head = atomic_load_explicit(&queue->head, memory_order_relaxed);
		}
	}

	*item = atomic_load_explicit(&cell->item, memory_order_relaxed);
	atomic_store_explicit(&cell->item, NULL, memory_order_relaxed);
	atomic_store_explicit(&cell->sequence, head + queue->capacity,
						  memory_order_release);
	return 0;
}

static inline size_t croutine_queue_steal_half(struct croutine_queue *victim,
											   struct croutine_queue *target,
											   void **first) {
	uint64_t head;
	uint64_t tail;
	uint64_t count;
	uint64_t target_head;
	uint64_t target_tail;
	uint64_t target_space;
	uint64_t steal_count;
	void *run_now = NULL;

	if (victim == NULL || target == NULL || first == NULL ||
		victim->capacity == 0 || target->capacity == 0)
		return 0;

	*first = NULL;
	for (;;) {
		head = atomic_load_explicit(&victim->head, memory_order_acquire);
		tail = atomic_load_explicit(&victim->tail, memory_order_acquire);
		count = tail - head;
		if (count == 0)
			return 0;
		if (count > victim->capacity)
			continue;

		steal_count = count - count / 2;
		target_head = atomic_load_explicit(&target->head, memory_order_acquire);
		target_tail = atomic_load_explicit(&target->tail, memory_order_relaxed);
		target_space = target->capacity - (target_tail - target_head);
		if (steal_count > target_space + 1)
			steal_count = target_space + 1;
		if (steal_count == 0)
			return 0;

		for (uint64_t index = 0; index < steal_count; index++) {
			croutine_queue_cell *source;
			uint64_t position = head + index;
			uint64_t sequence;
			void *item;

			source = croutine_queue_cell_at(victim, position);
			sequence =
				atomic_load_explicit(&source->sequence, memory_order_acquire);
			if (sequence != position + 1)
				return 0;

			item = atomic_load_explicit(&source->item, memory_order_relaxed);
			if (index + 1 == steal_count) {
				run_now = item;
			} else {
				croutine_queue_cell *dest;
				uint64_t dest_position = target_tail + index;
				uint64_t dest_sequence;

				dest = croutine_queue_cell_at(target, dest_position);
				dest_sequence =
					atomic_load_explicit(&dest->sequence, memory_order_acquire);
				if (dest_sequence != dest_position)
					return 0;
				atomic_store_explicit(&dest->item, item, memory_order_relaxed);
			}
		}

		if (!atomic_compare_exchange_strong_explicit(
				&victim->head, &head, head + steal_count, memory_order_acq_rel,
				memory_order_relaxed))
			continue;

		for (uint64_t index = 0; index < steal_count; index++) {
			croutine_queue_cell *source =
				croutine_queue_cell_at(victim, head + index);
			atomic_store_explicit(&source->item, NULL, memory_order_relaxed);
			atomic_store_explicit(&source->sequence,
								  head + index + victim->capacity,
								  memory_order_release);
		}
		for (uint64_t index = 0; index + 1 < steal_count; index++) {
			croutine_queue_cell *dest =
				croutine_queue_cell_at(target, target_tail + index);
			atomic_store_explicit(&dest->sequence, target_tail + index + 1,
								  memory_order_release);
		}
		if (steal_count > 1)
			atomic_store_explicit(&target->tail, target_tail + steal_count - 1,
								  memory_order_release);
		*first = run_now;
		return (size_t)steal_count;
	}
}

/*
 * Intrusive Doubly Linked List
 */

typedef struct croutine_list_head {
	struct croutine_list_head *prev;
	struct croutine_list_head *next;
} croutine_list_head;

#define croutine_list_entry(ptr, type, member) \
	croutine_container_of(ptr, type, member)

#define croutine_list_for_each(pos, head) \
	for ((pos) = (head)->next; (pos) != (head); (pos) = (pos)->next)

#define croutine_list_for_each_safe(pos, tmp, head)                  \
	for ((pos) = (head)->next, (tmp) = (pos)->next; (pos) != (head); \
		 (pos) = (tmp), (tmp) = (pos)->next)

static inline void croutine_list_init(struct croutine_list_head *head) {
	head->prev = head;
	head->next = head;
}

static inline int croutine_list_empty(const struct croutine_list_head *head) {
	return head->next == head;
}

static inline void
croutine_list_insert_between(struct croutine_list_head *prev,
							 struct croutine_list_head *next,
							 struct croutine_list_head *node) {
	node->prev = prev;
	node->next = next;
	prev->next = node;
	next->prev = node;
}

static inline void croutine_list_insert_after(struct croutine_list_head *pos,
											  struct croutine_list_head *node) {
	croutine_list_insert_between(pos, pos->next, node);
}

static inline void
croutine_list_insert_before(struct croutine_list_head *pos,
							struct croutine_list_head *node) {
	croutine_list_insert_between(pos->prev, pos, node);
}

static inline void croutine_list_push_front(struct croutine_list_head *head,
											struct croutine_list_head *node) {
	croutine_list_insert_after(head, node);
}

static inline void croutine_list_push_back(struct croutine_list_head *head,
										   struct croutine_list_head *node) {
	croutine_list_insert_before(head, node);
}

static inline void croutine_list_remove(struct croutine_list_head *node) {
	node->prev->next = node->next;
	node->next->prev = node->prev;
	croutine_list_init(node);
}

#endif
