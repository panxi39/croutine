#include "croutine_structures.h"

#include <stdio.h>

#define CHECK(condition, message)             \
	do {                                      \
		if (!(condition)) {                   \
			fprintf(stderr, "%s\n", message); \
			return -1;                        \
		}                                     \
	} while (0)

struct list_test_node {
	int value;
	struct croutine_list_head link;
};

struct refcount_test_node {
	struct croutine_refcount refs;
	int released;
};

static void refcount_test_release(struct croutine_refcount *refcount) {
	struct refcount_test_node *node;

	node = croutine_container_of(refcount, struct refcount_test_node, refs);
	node->released++;
}

static int test_helpers(void) {
	struct list_test_node node = { .value = 42 };
	struct croutine_list_head *link = &node.link;

	CHECK(croutine_container_of(link, struct list_test_node, link) == &node,
		  "container_of should recover containing object");

	return 0;
}

static int test_refcount(void) {
	struct refcount_test_node node = { 0 };

	croutine_refcount_init(&node.refs);
	CHECK(croutine_refcount_read(&node.refs) == 1,
		  "refcount should initialize to one");
	CHECK(node.released == 0, "refcount init should not release object");

	CHECK(croutine_refcount_get(&node.refs) == 0,
		  "refcount get should acquire a live reference");
	CHECK(croutine_refcount_read(&node.refs) == 2,
		  "refcount get should increment count");

	CHECK(croutine_refcount_release(&node.refs, refcount_test_release) == 0,
		  "first refcount release should not release object");
	CHECK(croutine_refcount_read(&node.refs) == 1,
		  "refcount should decrement after release");
	CHECK(node.released == 0,
		  "object should not release while refcount remains nonzero");

	CHECK(croutine_refcount_get(&node.refs) == 0,
		  "refcount get should acquire a live reference");
	CHECK(croutine_refcount_read(&node.refs) == 2,
		  "refcount get should increment live refcount");

	CHECK(croutine_refcount_release(&node.refs, refcount_test_release) == 0,
		  "release after get should not release with one ref left");
	CHECK(croutine_refcount_release(&node.refs, refcount_test_release) == 1,
		  "final refcount release should release object");
	CHECK(croutine_refcount_read(&node.refs) == 0,
		  "final release should leave refcount at zero");
	CHECK(node.released == 1, "release callback should run exactly once");

	CHECK(croutine_refcount_get(&node.refs) == -1,
		  "refcount get should reject a dead object");
	CHECK(croutine_refcount_read(&node.refs) == 0,
		  "failed get should not change zero refcount");
	CHECK(croutine_refcount_release(&node.refs, refcount_test_release) == -1,
		  "release on zero refcount should fail");
	CHECK(node.released == 1, "failed release should not release object again");

	return 0;
}

static int test_queue(void) {
	struct croutine_queue queue;
	int a = 1;
	int b = 2;
	int c = 3;
	int d = 4;
	void *item;

	CHECK(croutine_queue_init(NULL, 3) == -1,
		  "queue init should reject NULL queue");
	CHECK(croutine_queue_init(&queue, 0) == -1,
		  "queue init should reject zero capacity");
	CHECK(croutine_queue_init(&queue, 3) == 0, "queue init should succeed");

	CHECK(croutine_queue_empty(&queue), "queue should start empty");
	CHECK(!croutine_queue_full(&queue), "empty queue should not be full");
	CHECK(croutine_queue_pop(&queue, &item) == -1,
		  "pop from empty queue should fail");

	CHECK(croutine_queue_push(&queue, &a) == 0, "push a should succeed");
	CHECK(croutine_queue_push(&queue, &b) == 0, "push b should succeed");
	CHECK(croutine_queue_push(&queue, &c) == 0, "push c should succeed");
	CHECK(croutine_queue_full(&queue), "queue should be full");
	CHECK(croutine_queue_push(&queue, &d) == -1,
		  "push to full queue should fail");

	CHECK(croutine_queue_pop(&queue, &item) == 0, "pop a should succeed");
	CHECK(item == &a, "queue should preserve FIFO order for a");
	CHECK(!croutine_queue_full(&queue),
		  "queue should not be full after one pop");

	CHECK(croutine_queue_push(&queue, &d) == 0,
		  "push d should wrap through buffer");
	CHECK(croutine_queue_pop(&queue, &item) == 0, "pop b should succeed");
	CHECK(item == &b, "queue should preserve FIFO order for b");
	CHECK(croutine_queue_pop(&queue, &item) == 0, "pop c should succeed");
	CHECK(item == &c, "queue should preserve FIFO order for c");
	CHECK(croutine_queue_pop(&queue, &item) == 0, "pop d should succeed");
	CHECK(item == &d, "queue should preserve FIFO order for d");
	CHECK(croutine_queue_empty(&queue), "queue should be empty after pops");

	CHECK(croutine_queue_push(&queue, &a) == 0,
		  "push before reset should work");
	croutine_queue_reset(&queue);
	CHECK(croutine_queue_empty(&queue), "reset should empty queue");

	croutine_queue_destroy(&queue);
	CHECK(queue.buffer == NULL, "destroy should clear queue buffer");
	CHECK(queue.capacity == 0, "destroy should clear queue capacity");

	return 0;
}

static int test_list(void) {
	struct croutine_list_head head;
	struct croutine_list_head *pos;
	struct croutine_list_head *tmp;
	struct list_test_node first = { .value = 1 };
	struct list_test_node second = { .value = 2 };
	struct list_test_node third = { .value = 3 };
	int expected[] = { 3, 1, 2 };
	size_t index = 0;

	croutine_list_init(&head);
	croutine_list_init(&first.link);
	croutine_list_init(&second.link);
	croutine_list_init(&third.link);

	CHECK(croutine_list_empty(&head), "list should start empty");

	croutine_list_push_back(&head, &first.link);
	croutine_list_push_back(&head, &second.link);
	croutine_list_push_front(&head, &third.link);

	croutine_list_for_each(pos, &head) {
		struct list_test_node *node;

		CHECK(index < sizeof(expected) / sizeof(expected[0]),
			  "list contains too many nodes");
		node = croutine_list_entry(pos, struct list_test_node, link);
		CHECK(node->value == expected[index], "list iteration order is wrong");
		index++;
	}
	CHECK(index == sizeof(expected) / sizeof(expected[0]),
		  "list contains too few nodes");

	croutine_list_remove(&first.link);
	CHECK(first.link.next == &first.link,
		  "removed node next should point to itself");
	CHECK(first.link.prev == &first.link,
		  "removed node prev should point to itself");

	index = 0;
	croutine_list_for_each_safe(pos, tmp, &head) {
		croutine_list_remove(pos);
		index++;
	}
	CHECK(index == 2, "safe iteration should visit remaining nodes");
	CHECK(croutine_list_empty(&head), "list should be empty after removals");

	return 0;
}

int main(void) {
	if (test_helpers() != 0)
		return 1;

	if (test_refcount() != 0)
		return 1;

	if (test_queue() != 0)
		return 1;

	if (test_list() != 0)
		return 1;

	return 0;
}
