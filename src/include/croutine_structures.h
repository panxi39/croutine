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

typedef void (*croutine_refcount_release_fn)(struct croutine_refcount *refcount);

static inline void croutine_refcount_init(struct croutine_refcount *refcount) {
	atomic_init(&refcount->refs, 1);
}

static inline uint32_t croutine_refcount_read(const struct croutine_refcount *refcount) {
	return atomic_load_explicit(&refcount->refs, memory_order_acquire);
}

static inline int croutine_refcount_get(struct croutine_refcount *refcount) {
	uint32_t refs;

	refs = atomic_load_explicit(&refcount->refs, memory_order_acquire);
	while (refs != 0) {
		if (atomic_compare_exchange_weak_explicit(&refcount->refs, &refs, refs + 1, memory_order_acquire,
												  memory_order_relaxed))
			return 0;
	}

	return -1;
}

static inline int croutine_refcount_release(struct croutine_refcount *refcount, croutine_refcount_release_fn release) {
	uint32_t refs;

	refs = atomic_load_explicit(&refcount->refs, memory_order_acquire);
	while (refs != 0) {
		if (!atomic_compare_exchange_weak_explicit(&refcount->refs, &refs, refs - 1, memory_order_release,
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
 * Intrusive Doubly Linked List
 */

typedef struct croutine_list_head {
	struct croutine_list_head *prev;
	struct croutine_list_head *next;
} croutine_list_head;

#define croutine_list_entry(ptr, type, member) croutine_container_of(ptr, type, member)

#define croutine_list_for_each(pos, head) for ((pos) = (head)->next; (pos) != (head); (pos) = (pos)->next)

#define croutine_list_for_each_safe(pos, tmp, head) \
	for ((pos) = (head)->next, (tmp) = (pos)->next; (pos) != (head); (pos) = (tmp), (tmp) = (pos)->next)

static inline void croutine_list_init(struct croutine_list_head *head) {
	head->prev = head;
	head->next = head;
}

static inline int croutine_list_empty(const struct croutine_list_head *head) {
	return head->next == head;
}

static inline void croutine_list_insert_between(struct croutine_list_head *prev, struct croutine_list_head *next,
												struct croutine_list_head *node) {
	node->prev = prev;
	node->next = next;
	prev->next = node;
	next->prev = node;
}

static inline void croutine_list_insert_after(struct croutine_list_head *pos, struct croutine_list_head *node) {
	croutine_list_insert_between(pos, pos->next, node);
}

static inline void croutine_list_insert_before(struct croutine_list_head *pos, struct croutine_list_head *node) {
	croutine_list_insert_between(pos->prev, pos, node);
}

static inline void croutine_list_push_front(struct croutine_list_head *head, struct croutine_list_head *node) {
	croutine_list_insert_after(head, node);
}

static inline void croutine_list_push_back(struct croutine_list_head *head, struct croutine_list_head *node) {
	croutine_list_insert_before(head, node);
}

static inline void croutine_list_remove(struct croutine_list_head *node) {
	node->prev->next = node->next;
	node->next->prev = node->prev;
	croutine_list_init(node);
}

#endif
