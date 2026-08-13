#include "queue.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(condition, message)                                        \
	do {                                                                 \
		if (!(condition)) {                                              \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, message); \
			return -1;                                                   \
		}                                                                \
	} while (0)

#define MPMC_PRODUCER_COUNT 4
#define MPMC_CONSUMER_COUNT 4
#define MPMC_STRESS_ITEMS_PER_PRODUCER 16384

#define CLDEQUE_THIEF_COUNT 4
#define CLDEQUE_STRESS_ITEMS 65536

#define QUEUE_STRESS_IDLE_LIMIT 1000000

typedef struct queue_stress_state {
	size_t *values;
	_Atomic unsigned char *seen;
	size_t total;
	_Atomic size_t consumed;
	_Atomic int failed;
} queue_stress_state;

static void queue_stress_pause(size_t *counter) {
	if (((*counter)++ & 63) == 0)
		sched_yield();
}

static void queue_stress_fail(queue_stress_state *state, const char *message) {
	int expected = 0;

	if (atomic_compare_exchange_strong_explicit(&state->failed, &expected, 1, memory_order_acq_rel,
												memory_order_acquire))
		fprintf(stderr, "%s\n", message);
}

static int queue_stress_init(queue_stress_state *state, size_t total) {
	state->values = malloc(total * sizeof(state->values[0]));
	state->seen = malloc(total * sizeof(state->seen[0]));
	if (state->values == NULL || state->seen == NULL) {
		free(state->values);
		free(state->seen);
		return -1;
	}

	state->total = total;
	atomic_init(&state->consumed, 0);
	atomic_init(&state->failed, 0);
	for (size_t index = 0; index < total; index++) {
		state->values[index] = index;
		atomic_init(&state->seen[index], 0);
	}

	return 0;
}

static void queue_stress_destroy(queue_stress_state *state) {
	free(state->values);
	free(state->seen);
	state->values = NULL;
	state->seen = NULL;
	state->total = 0;
}

static int queue_stress_mark(queue_stress_state *state, void *item) {
	uintptr_t base;
	uintptr_t address;
	uintptr_t offset;
	size_t index;
	unsigned char seen;
	size_t consumed;

	if (item == NULL) {
		queue_stress_fail(state, "stress test popped NULL item");
		return -1;
	}

	base = (uintptr_t)state->values;
	address = (uintptr_t)item;
	if (address < base) {
		queue_stress_fail(state, "stress test popped item before value range");
		return -1;
	}

	offset = address - base;
	if (offset % sizeof(state->values[0]) != 0) {
		queue_stress_fail(state, "stress test popped unaligned item pointer");
		return -1;
	}

	index = offset / sizeof(state->values[0]);
	if (index >= state->total) {
		queue_stress_fail(state, "stress test popped item after value range");
		return -1;
	}
	if (state->values[index] != index) {
		queue_stress_fail(state, "stress test popped corrupted item value");
		return -1;
	}

	seen = atomic_exchange_explicit(&state->seen[index], 1, memory_order_acq_rel);
	if (seen != 0) {
		queue_stress_fail(state, "stress test popped duplicate item");
		return -1;
	}

	consumed = atomic_fetch_add_explicit(&state->consumed, 1, memory_order_acq_rel);
	if (consumed >= state->total) {
		queue_stress_fail(state, "stress test consumed too many items");
		return -1;
	}

	return 0;
}

static int queue_stress_verify(queue_stress_state *state) {
	CHECK(atomic_load_explicit(&state->failed, memory_order_acquire) == 0,
		  "stress test reported a queue integrity failure");
	CHECK(atomic_load_explicit(&state->consumed, memory_order_acquire) == state->total,
		  "stress test did not consume all items");

	for (size_t index = 0; index < state->total; index++) {
		CHECK(atomic_load_explicit(&state->seen[index], memory_order_acquire) == 1, "stress test missed an item");
	}

	return 0;
}

/*
 * MPMC queue tests
 */

static int test_mpmc_queue(void) {
	croutine_mpmc_queue *queue;
	int a = 1;
	int b = 2;
	int c = 3;
	int d = 4;
	int e = 5;

	CHECK(croutine_mpmc_queue_init(0) == NULL, "queue init should reject zero capacity");
	CHECK(croutine_mpmc_queue_init(1) == NULL, "queue init should reject capacity 1");
	CHECK(croutine_mpmc_queue_init(3) == NULL, "queue init should reject odd capacity 3");

	queue = croutine_mpmc_queue_init(4);
	CHECK(queue != NULL, "queue init should succeed for power-of-two capacity");

	CHECK(croutine_mpmc_queue_pop(queue) == NULL, "pop from empty queue should return NULL");

	CHECK(croutine_mpmc_queue_push(queue, &a) == 1, "push a should succeed");
	CHECK(croutine_mpmc_queue_push(queue, &b) == 1, "push b should succeed");
	CHECK(croutine_mpmc_queue_push(queue, &c) == 1, "push c should succeed");
	CHECK(croutine_mpmc_queue_push(queue, &d) == 1, "push d should succeed");
	CHECK(croutine_mpmc_queue_push(queue, &e) == -1, "push to full queue should fail");

	CHECK(croutine_mpmc_queue_pop(queue) == &a, "pop a should succeed and preserve FIFO order");

	CHECK(croutine_mpmc_queue_push(queue, &e) == 1, "push e should wrap through buffer");
	CHECK(croutine_mpmc_queue_pop(queue) == &b, "pop b should succeed");
	CHECK(croutine_mpmc_queue_pop(queue) == &c, "pop c should succeed");
	CHECK(croutine_mpmc_queue_pop(queue) == &d, "pop d should succeed");
	CHECK(croutine_mpmc_queue_pop(queue) == &e, "pop e should succeed");
	CHECK(croutine_mpmc_queue_pop(queue) == NULL, "queue should be empty after pops");

	croutine_mpmc_queue_destroy(queue);
	return 0;
}

typedef struct mpmc_producer_context {
	croutine_mpmc_queue *queue;
	queue_stress_state *state;
	_Atomic int *start;
	_Atomic size_t *producers_done;
	size_t first;
	size_t last;
} mpmc_producer_context;

typedef struct mpmc_consumer_context {
	croutine_mpmc_queue *queue;
	queue_stress_state *state;
	_Atomic int *start;
	_Atomic size_t *producers_done;
} mpmc_consumer_context;

static void *mpmc_producer_main(void *arg) {
	mpmc_producer_context *context = arg;
	size_t next = context->first;
	size_t pause = 0;

	while (atomic_load_explicit(context->start, memory_order_acquire) == 0)
		sched_yield();

	while (next < context->last && atomic_load_explicit(&context->state->failed, memory_order_acquire) == 0) {
		if (croutine_mpmc_queue_push(context->queue, &context->state->values[next]) == 1) {
			next++;
		} else {
			queue_stress_pause(&pause);
		}
	}

	atomic_fetch_add_explicit(context->producers_done, 1, memory_order_acq_rel);
	return NULL;
}

static void *mpmc_consumer_main(void *arg) {
	mpmc_consumer_context *context = arg;
	size_t idle = 0;
	size_t pause = 0;

	while (atomic_load_explicit(context->start, memory_order_acquire) == 0)
		sched_yield();

	for (;;) {
		size_t consumed;

		if (atomic_load_explicit(&context->state->failed, memory_order_acquire) != 0)
			break;

		consumed = atomic_load_explicit(&context->state->consumed, memory_order_acquire);
		if (consumed >= context->state->total)
			break;

		void *item = croutine_mpmc_queue_pop(context->queue);
		if (item != NULL) {
			idle = 0;
			(void)queue_stress_mark(context->state, item);
			continue;
		}

		if (atomic_load_explicit(context->producers_done, memory_order_acquire) == MPMC_PRODUCER_COUNT &&
			++idle > QUEUE_STRESS_IDLE_LIMIT) {
			queue_stress_fail(context->state, "mpmc stress test stopped making progress");
			break;
		}
		queue_stress_pause(&pause);
	}

	return NULL;
}

static int test_mpmc_queue_concurrent_capacity(size_t capacity, size_t items_per_producer) {
	croutine_mpmc_queue *queue;
	queue_stress_state state;
	pthread_t producers[MPMC_PRODUCER_COUNT];
	pthread_t consumers[MPMC_CONSUMER_COUNT];
	mpmc_producer_context producer_contexts[MPMC_PRODUCER_COUNT];
	mpmc_consumer_context consumer_contexts[MPMC_CONSUMER_COUNT];
	_Atomic int start;
	_Atomic size_t producers_done;
	size_t total = MPMC_PRODUCER_COUNT * items_per_producer;
	size_t producer_count = 0;
	size_t consumer_count = 0;
	int ret = -1;

	CHECK(queue_stress_init(&state, total) == 0, "mpmc stress test should allocate state");
	queue = croutine_mpmc_queue_init(capacity);
	CHECK(queue != NULL, "mpmc stress test should initialize queue");

	atomic_init(&start, 0);
	atomic_init(&producers_done, 0);
	for (size_t index = 0; index < MPMC_PRODUCER_COUNT; index++) {
		producer_contexts[index].queue = queue;
		producer_contexts[index].state = &state;
		producer_contexts[index].start = &start;
		producer_contexts[index].producers_done = &producers_done;
		producer_contexts[index].first = index * items_per_producer;
		producer_contexts[index].last = producer_contexts[index].first + items_per_producer;
		if (pthread_create(&producers[index], NULL, mpmc_producer_main, &producer_contexts[index]) != 0) {
			queue_stress_fail(&state, "mpmc stress test failed to create producer");
			goto out_start;
		}
		producer_count++;
	}
	for (size_t index = 0; index < MPMC_CONSUMER_COUNT; index++) {
		consumer_contexts[index].queue = queue;
		consumer_contexts[index].state = &state;
		consumer_contexts[index].start = &start;
		consumer_contexts[index].producers_done = &producers_done;
		if (pthread_create(&consumers[index], NULL, mpmc_consumer_main, &consumer_contexts[index]) != 0) {
			queue_stress_fail(&state, "mpmc stress test failed to create consumer");
			goto out_start;
		}
		consumer_count++;
	}

out_start:
	atomic_store_explicit(&start, 1, memory_order_release);
	for (size_t index = 0; index < producer_count; index++)
		(void)pthread_join(producers[index], NULL);
	for (size_t index = 0; index < consumer_count; index++)
		(void)pthread_join(consumers[index], NULL);

	if (producer_count == MPMC_PRODUCER_COUNT && consumer_count == MPMC_CONSUMER_COUNT &&
		queue_stress_verify(&state) == 0)
		ret = 0;

	croutine_mpmc_queue_destroy(queue);
	queue_stress_destroy(&state);
	return ret;
}

static int test_mpmc_queue_concurrent(void) {
	const struct {
		size_t capacity;
		size_t items_per_producer;
	} configs[] = {
		{ 2, MPMC_STRESS_ITEMS_PER_PRODUCER },
		{ 4, MPMC_STRESS_ITEMS_PER_PRODUCER },
		{ 64, MPMC_STRESS_ITEMS_PER_PRODUCER },
	};

	for (size_t index = 0; index < sizeof(configs) / sizeof(configs[0]); index++) {
		CHECK(test_mpmc_queue_concurrent_capacity(configs[index].capacity, configs[index].items_per_producer) == 0,
			  "mpmc stress test should pass");
	}

	return 0;
}

/*
 * Work-stealing deque tests
 */

static int test_cldeque(void) {
	croutine_cldeque *deque;
	int a = 1;
	int b = 2;
	int c = 3;
	int d = 4;
	int e = 5;
	int f = 6;
	int g = 7;

	CHECK(croutine_cldeque_init(0) == NULL, "deque init should reject zero capacity");
	CHECK(croutine_cldeque_init(3) == NULL, "deque init should reject non power-of-two capacity 3");
	CHECK(croutine_cldeque_init(6) == NULL, "deque init should reject non power-of-two capacity 6");
	CHECK(croutine_cldeque_init(1) == NULL, "deque init should reject capacity 1");
	CHECK(croutine_cldeque_len(NULL) == 0, "NULL deque length should be zero");

	deque = croutine_cldeque_init(2);
	CHECK(deque != NULL, "deque init should succeed for power-of-two capacity");
	CHECK(croutine_cldeque_len(deque) == 0, "new deque length should be zero");

	CHECK(croutine_cldeque_pop(deque) == NULL, "pop from empty deque should return NULL");
	CHECK(croutine_cldeque_steal(deque) == NULL, "steal from empty deque should return NULL");
	CHECK(croutine_cldeque_len(deque) == 0, "empty operations should preserve zero length");

	CHECK(croutine_cldeque_push(deque, &a) == 1, "push a should succeed");
	CHECK(croutine_cldeque_len(deque) == 1, "deque length should be one after pushing a");
	CHECK(croutine_cldeque_push(deque, &b) == 1, "push b should succeed");
	CHECK(croutine_cldeque_len(deque) == 2, "deque length should reach capacity");
	CHECK(croutine_cldeque_push(deque, &c) == -1, "push to full deque should fail");
	CHECK(croutine_cldeque_len(deque) == 2, "failed push should preserve deque length");

	CHECK(croutine_cldeque_pop(deque) == &b, "pop should return the newest item (LIFO)");
	CHECK(croutine_cldeque_len(deque) == 1, "pop should decrement deque length");
	CHECK(croutine_cldeque_steal(deque) == &a, "steal should return the oldest item (FIFO)");
	CHECK(croutine_cldeque_len(deque) == 0, "steal should drain the deque");
	CHECK(croutine_cldeque_pop(deque) == NULL, "deque should be empty after pop and steal");
	CHECK(croutine_cldeque_steal(deque) == NULL, "steal from drained deque should return NULL");

	CHECK(croutine_cldeque_push(deque, &c) == 1, "push c should succeed");
	CHECK(croutine_cldeque_steal(deque) == &c, "single element steal should succeed");
	CHECK(croutine_cldeque_push(deque, &d) == 1, "push d should succeed");
	CHECK(croutine_cldeque_pop(deque) == &d, "single element pop should succeed");
	CHECK(croutine_cldeque_pop(deque) == NULL, "deque should be empty after single element rounds");

	croutine_cldeque_destroy(deque);

	deque = croutine_cldeque_init(4);
	CHECK(deque != NULL, "deque init should succeed for power-of-two capacity");

	CHECK(croutine_cldeque_push(deque, &a) == 1, "push a should succeed");
	CHECK(croutine_cldeque_push(deque, &b) == 1, "push b should succeed");
	CHECK(croutine_cldeque_push(deque, &c) == 1, "push c should succeed");
	CHECK(croutine_cldeque_push(deque, &d) == 1, "push d should succeed");
	CHECK(croutine_cldeque_push(deque, &e) == -1, "push to full deque should fail");

	CHECK(croutine_cldeque_steal(deque) == &a, "steal a should succeed");
	CHECK(croutine_cldeque_pop(deque) == &d, "pop d should succeed");
	CHECK(croutine_cldeque_push(deque, &e) == 1, "push e should wrap through buffer");
	CHECK(croutine_cldeque_push(deque, &f) == 1, "push f should wrap through buffer");
	CHECK(croutine_cldeque_push(deque, &g) == -1, "push to refilled deque should fail");

	CHECK(croutine_cldeque_steal(deque) == &b, "steal b should succeed");
	CHECK(croutine_cldeque_steal(deque) == &c, "steal c should succeed");
	CHECK(croutine_cldeque_pop(deque) == &f, "pop f should succeed");
	CHECK(croutine_cldeque_pop(deque) == &e, "pop e should succeed");
	CHECK(croutine_cldeque_pop(deque) == NULL, "deque should be empty after mixed drains");
	CHECK(croutine_cldeque_steal(deque) == NULL, "steal from drained deque should return NULL");

	croutine_cldeque_destroy(deque);
	return 0;
}

static int test_cldeque_index_wrap(void) {
	croutine_cldeque *deque;
	const uint64_t start = UINT64_MAX - 1;
	int a = 1;
	int b = 2;
	int c = 3;
	int d = 4;
	int e = 5;

	deque = croutine_cldeque_init(4);
	CHECK(deque != NULL, "wrap test should initialize deque");

	atomic_store_explicit(&deque->head, start, memory_order_relaxed);
	atomic_store_explicit(&deque->tail, start, memory_order_relaxed);
	CHECK(croutine_cldeque_len(deque) == 0, "wrapped empty deque length should be zero");

	CHECK(croutine_cldeque_push(deque, &a) == 1, "wrap push a should succeed");
	CHECK(croutine_cldeque_push(deque, &b) == 1, "wrap push b should cross UINT64_MAX");
	CHECK(croutine_cldeque_push(deque, &c) == 1, "wrap push c should succeed after index rollover");
	CHECK(croutine_cldeque_push(deque, &d) == 1, "wrap push d should fill deque");
	CHECK(croutine_cldeque_len(deque) == 4, "wrapped full deque length should equal capacity");
	CHECK(croutine_cldeque_push(deque, &e) == -1, "wrapped full deque should reject another push");

	CHECK(croutine_cldeque_steal(deque) == &a, "wrapped steal should return oldest item");
	CHECK(croutine_cldeque_len(deque) == 3, "wrapped steal should decrement length");
	CHECK(croutine_cldeque_pop(deque) == &d, "wrapped pop should return newest item");
	CHECK(croutine_cldeque_len(deque) == 2, "wrapped pop should decrement length");
	CHECK(croutine_cldeque_steal(deque) == &b, "steal should advance head through rollover");
	CHECK(croutine_cldeque_pop(deque) == &c, "pop should drain final wrapped item");
	CHECK(croutine_cldeque_len(deque) == 0, "drained wrapped deque length should be zero");
	CHECK(croutine_cldeque_pop(deque) == NULL, "pop from drained wrapped deque should be empty");
	CHECK(croutine_cldeque_steal(deque) == NULL, "steal from drained wrapped deque should be empty");

	CHECK(croutine_cldeque_push(deque, &e) == 1, "deque should remain reusable after index rollover");
	CHECK(croutine_cldeque_pop(deque) == &e, "reused wrapped deque should preserve item");
	CHECK(croutine_cldeque_len(deque) == 0, "reused wrapped deque should drain cleanly");

	croutine_cldeque_destroy(deque);
	return 0;
}

typedef struct cldeque_owner_context {
	croutine_cldeque *deque;
	queue_stress_state *state;
	_Atomic int *start;
	_Atomic int *owner_done;
} cldeque_owner_context;

typedef struct cldeque_thief_context {
	croutine_cldeque *deque;
	queue_stress_state *state;
	_Atomic int *start;
	_Atomic int *owner_done;
} cldeque_thief_context;

/*
 * The owner is the only caller of push/pop, as required by the deque
 * contract. It feeds items while there is room and drains from the tail
 * otherwise, so progress is always possible.
 */
static void *cldeque_owner_main(void *arg) {
	cldeque_owner_context *context = arg;
	size_t next = 0;
	size_t pause = 0;

	while (atomic_load_explicit(context->start, memory_order_acquire) == 0)
		sched_yield();

	for (;;) {
		size_t consumed;
		void *item;

		if (atomic_load_explicit(&context->state->failed, memory_order_acquire) != 0)
			break;

		consumed = atomic_load_explicit(&context->state->consumed, memory_order_acquire);
		if (consumed >= context->state->total && next >= context->state->total)
			break;

		if (next < context->state->total && croutine_cldeque_push(context->deque, &context->state->values[next]) == 1) {
			next++;
			continue;
		}

		item = croutine_cldeque_pop(context->deque);
		if (item != NULL) {
			(void)queue_stress_mark(context->state, item);
			continue;
		}

		if (next >= context->state->total)
			break;

		queue_stress_pause(&pause);
	}

	atomic_store_explicit(context->owner_done, 1, memory_order_release);
	return NULL;
}

static void *cldeque_thief_main(void *arg) {
	cldeque_thief_context *context = arg;
	size_t idle = 0;
	size_t pause = 0;

	while (atomic_load_explicit(context->start, memory_order_acquire) == 0)
		sched_yield();

	for (;;) {
		void *item;

		if (atomic_load_explicit(&context->state->failed, memory_order_acquire) != 0)
			break;

		if (atomic_load_explicit(&context->state->consumed, memory_order_acquire) >= context->state->total)
			break;

		item = croutine_cldeque_steal(context->deque);
		if (item != NULL) {
			idle = 0;
			(void)queue_stress_mark(context->state, item);
			continue;
		}

		if (atomic_load_explicit(context->owner_done, memory_order_acquire) != 0 && ++idle > QUEUE_STRESS_IDLE_LIMIT) {
			queue_stress_fail(context->state, "cldeque stress test stopped making progress");
			break;
		}
		queue_stress_pause(&pause);
	}

	return NULL;
}

static int test_cldeque_concurrent_capacity(size_t capacity, size_t total, uint64_t initial_index) {
	croutine_cldeque *deque;
	queue_stress_state state;
	pthread_t owner;
	pthread_t thieves[CLDEQUE_THIEF_COUNT];
	cldeque_owner_context owner_context;
	cldeque_thief_context thief_contexts[CLDEQUE_THIEF_COUNT];
	_Atomic int start;
	_Atomic int owner_done;
	size_t thief_count = 0;
	int owner_started = 0;
	int ret = -1;

	CHECK(queue_stress_init(&state, total) == 0, "cldeque stress test should allocate state");
	deque = croutine_cldeque_init(capacity);
	CHECK(deque != NULL, "cldeque stress test should initialize deque");
	atomic_store_explicit(&deque->head, initial_index, memory_order_relaxed);
	atomic_store_explicit(&deque->tail, initial_index, memory_order_relaxed);

	atomic_init(&start, 0);
	atomic_init(&owner_done, 0);

	owner_context.deque = deque;
	owner_context.state = &state;
	owner_context.start = &start;
	owner_context.owner_done = &owner_done;
	if (pthread_create(&owner, NULL, cldeque_owner_main, &owner_context) != 0) {
		queue_stress_fail(&state, "cldeque stress test failed to create owner");
	} else {
		owner_started = 1;
	}

	for (size_t index = 0; index < CLDEQUE_THIEF_COUNT; index++) {
		thief_contexts[index].deque = deque;
		thief_contexts[index].state = &state;
		thief_contexts[index].start = &start;
		thief_contexts[index].owner_done = &owner_done;
		if (pthread_create(&thieves[index], NULL, cldeque_thief_main, &thief_contexts[index]) != 0) {
			queue_stress_fail(&state, "cldeque stress test failed to create thief");
			break;
		}
		thief_count++;
	}

	atomic_store_explicit(&start, 1, memory_order_release);
	if (owner_started != 0)
		(void)pthread_join(owner, NULL);
	for (size_t index = 0; index < thief_count; index++)
		(void)pthread_join(thieves[index], NULL);

	if (owner_started != 0 && thief_count == CLDEQUE_THIEF_COUNT && queue_stress_verify(&state) == 0)
		ret = 0;

	croutine_cldeque_destroy(deque);
	queue_stress_destroy(&state);
	return ret;
}

static int test_cldeque_concurrent(void) {
	const struct {
		size_t capacity;
		size_t items;
		uint64_t initial_index;
	} configs[] = {
		{ 2, CLDEQUE_STRESS_ITEMS, 0 },
		{ 16, CLDEQUE_STRESS_ITEMS, UINT64_MAX - 1024 },
		{ 256, CLDEQUE_STRESS_ITEMS, UINT64_MAX - 1024 },
	};

	for (size_t index = 0; index < sizeof(configs) / sizeof(configs[0]); index++) {
		CHECK(test_cldeque_concurrent_capacity(configs[index].capacity, configs[index].items,
											   configs[index].initial_index) == 0,
			  "cldeque stress test should pass");
	}

	return 0;
}

int main(void) {
	if (test_mpmc_queue() != 0)
		return 1;

	if (test_mpmc_queue_concurrent() != 0)
		return 1;

	if (test_cldeque() != 0)
		return 1;

	if (test_cldeque_index_wrap() != 0)
		return 1;

	if (test_cldeque_concurrent() != 0)
		return 1;

	printf("All queue tests passed successfully!\n");
	return 0;
}
