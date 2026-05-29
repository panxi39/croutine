#include "arch.h"
#include "croutine_structures.h"
#include "stack.h"
#include "task.h"
#include "types.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define DEFAULT_TASK_COUNT 3
#define DEFAULT_REPEAT_COUNT 4
#define MAX_TASK_COUNT 1280
#define MAX_REPEAT_COUNT 1000
#define TASK_STACK_SIZE (64 * 1024)

struct test_task {
	struct croutine_task task;
	int arg;
	size_t id;
	size_t repeat;
	size_t ran;
	size_t value;
};

struct run {
	struct croutine_arch_context root;
	struct croutine_queue queue;
	struct test_task *tasks;
	size_t task_count;
	size_t repeat;
	size_t finished;
	uint32_t seed;
	int failed;
};

struct config {
	struct test_task *tasks;
	size_t task_count;
	size_t repeat;
	int status;
};

static _Thread_local struct run *active_run;

static void schedule(void);

static size_t next_value(void) {
	active_run->seed = active_run->seed * 1103515245u + 12345u;
	return active_run->seed;
}

static void fail(const char *message) {
	active_run->failed = 1;
	fprintf(stderr, "%s\n", message);
}

static void yield(void) {
	struct croutine_task *task = croutine_task_current();

	croutine_arch_store_and_call(&task->context, schedule);
}

static struct test_task *current_test_task(void) {
	struct croutine_task *task = croutine_task_current();

	if (task == NULL)
		return NULL;

	return croutine_container_of(task, struct test_task, task);
}

static void *task_entry(void *arg) {
	struct test_task *task = current_test_task();
	int id = *(int *)arg;
	size_t index;

	if (task == NULL || id < 0 || (size_t)id != task->id) {
		fail("task argument does not match task id");
		return NULL;
	}

	for (index = 0; index < task->repeat; index++) {
		task->ran++;
		printf("task %zu iteration %zu/%zu\n", task->id, index + 1,
			   task->repeat);
		yield();
	}

	task->value = next_value();
	printf("task %zu return %zu\n", task->id, task->value);
	return &task->value;
}

__attribute__((used)) static void task_finish(void *result) {
	struct croutine_task *task = croutine_task_current();
	struct test_task *test_task = current_test_task();

	task->result = result;
	task->state = CROUTINE_TASK_FINISHED;

	printf("task %zu returned %zu\n", test_task->id, *(size_t *)task->result);

	croutine_arch_store_and_call(&task->context, schedule);
	abort();
}

static void task_call_entry(void) {
	struct croutine_task *task = croutine_task_current();
	void *result;

	result = task->func(task->arg);
	task_finish(result);
}

static void schedule(void) {
	struct croutine_task *current = croutine_task_current();
	void *item;

	if (current != NULL) {
		struct test_task *task = current_test_task();

		if (current->state == CROUTINE_TASK_FINISHED) {
			active_run->finished++;
		} else if (!active_run->failed &&
				   croutine_queue_push(&active_run->queue, task) != 0) {
			fail("run queue is full");
		}
	}

	if (!active_run->failed &&
		croutine_queue_pop(&active_run->queue, &item) == 0) {
		struct test_task *task = item;

		croutine_task_init_current(&task->task);
		croutine_arch_resume_and_ret(&task->task.context);
	}

	croutine_task_init_current(NULL);
	croutine_arch_resume_and_ret(&active_run->root);
}

static int init_run(struct run *run, struct config *config) {
	size_t index;

	run->tasks = config->tasks;
	run->task_count = config->task_count;
	run->repeat = config->repeat;
	run->seed = (uint32_t)time(NULL);

	if (croutine_queue_init(&run->queue, (uint32_t)run->task_count) != 0)
		return -1;

	for (index = 0; index < run->task_count; index++) {
		struct test_task *test_task = &run->tasks[index];
		struct croutine_task *task = &test_task->task;

		test_task->id = index;
		test_task->arg = (int)index;
		test_task->repeat = run->repeat;
		task->func = task_entry;
		task->arg = &test_task->arg;
		task->state = CROUTINE_TASK_PENDING;

		if (task->stack == NULL || task->stack == (struct croutine_stack *)-1 ||
			croutine_arch_context_init(
				&task->context, task->stack->bottom, task->stack->size,
				(croutine_arch_entry)task_call_entry) != 0)
			return -1;

		if (croutine_queue_push(&run->queue, test_task) != 0)
			return -1;
	}

	return 0;
}

static int verify_run(const struct run *run) {
	size_t index;

	if (run->failed || run->finished != run->task_count)
		return -1;

	for (index = 0; index < run->task_count; index++) {
		const struct test_task *task = &run->tasks[index];

		if (task->task.state != CROUTINE_TASK_FINISHED ||
			task->task.result != &task->value || task->ran != run->repeat)
			return -1;
	}

	return 0;
}

static void *thread_main(void *arg) {
	struct config *config = arg;
	struct run run = { 0 };

	active_run = &run;
	croutine_task_init_current(NULL);

	if (init_run(&run, config) != 0) {
		config->status = 1;
		croutine_queue_destroy(&run.queue);
		return NULL;
	}

	croutine_arch_store_and_call(&run.root, schedule);

	config->status = verify_run(&run) == 0 ? 0 : 1;
	croutine_queue_destroy(&run.queue);
	active_run = NULL;
	croutine_task_init_current(NULL);
	return NULL;
}

static int parse_count(const char *text, size_t *value) {
	char *end;
	unsigned long long parsed;

	errno = 0;
	parsed = strtoull(text, &end, 10);
	if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
		parsed > SIZE_MAX)
		return -1;

	*value = (size_t)parsed;
	return 0;
}

static void destroy_tasks(struct test_task *tasks, size_t count) {
	size_t index;

	if (tasks == NULL)
		return;

	for (index = 0; index < count; index++)
		croutine_stack_free(tasks[index].task.stack);
	free(tasks);
}

static struct test_task *create_tasks(size_t count) {
	struct test_task *tasks;
	size_t index;

	tasks = calloc(count, sizeof(tasks[0]));
	if (tasks == NULL)
		return NULL;

	for (index = 0; index < count; index++) {
		tasks[index].task.stack = croutine_stack_alloc(TASK_STACK_SIZE);
		if (tasks[index].task.stack == (struct croutine_stack *)-1) {
			destroy_tasks(tasks, count);
			return NULL;
		}
	}

	return tasks;
}

int main(int argc, char **argv) {
	struct config config = {
		.task_count = DEFAULT_TASK_COUNT,
		.repeat = DEFAULT_REPEAT_COUNT,
	};
	pthread_t thread;
	size_t index;
	int ret;

	if (argc != 1 && argc != 3) {
		fprintf(stderr, "usage: %s [task_count repeat_count]\n", argv[0]);
		return 1;
	}

	if (argc == 3 && (parse_count(argv[1], &config.task_count) != 0 ||
					  parse_count(argv[2], &config.repeat) != 0)) {
		fprintf(stderr, "arguments must be positive integers\n");
		return 1;
	}

	if (config.task_count > MAX_TASK_COUNT ||
		config.repeat > MAX_REPEAT_COUNT) {
		fprintf(stderr, "test parameters are out of range\n");
		return 1;
	}

	config.tasks = create_tasks(config.task_count);
	if (config.tasks == NULL) {
		fprintf(stderr, "failed to allocate tasks\n");
		return 1;
	}

	ret = pthread_create(&thread, NULL, thread_main, &config);
	if (ret != 0) {
		fprintf(stderr, "pthread_create failed: %d\n", ret);
		destroy_tasks(config.tasks, config.task_count);
		return 1;
	}

	ret = pthread_join(thread, NULL);
	if (ret != 0) {
		fprintf(stderr, "pthread_join failed: %d\n", ret);
		destroy_tasks(config.tasks, config.task_count);
		return 1;
	}

	if (config.status != 0) {
		fprintf(stderr, "scheduler test failed\n");
		destroy_tasks(config.tasks, config.task_count);
		return 1;
	}

	for (index = 0; index < config.task_count; index++) {
		printf("main task %zu result %zu\n", index,
			   *(size_t *)config.tasks[index].task.result);
	}

	destroy_tasks(config.tasks, config.task_count);
	return 0;
}
