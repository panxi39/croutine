#define _POSIX_C_SOURCE 200809L

#include "croutine_event.h"
#include "croutine_structures.h"
#include "task.h"
#include "types.h"
#include "wait.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define RUNTIME_WORKERS 8
#define INITIAL_TASKS 64
#define PRODUCER_TASKS 4
#define TOTAL_TASKS (INITIAL_TASKS + PRODUCER_TASKS)
#define WAIT_ROUNDS 30000
#define STOP_AFTER_WAITS INITIAL_TASKS
#define STOP_PAUSE_US 650000L
#define FIRST_WAIT_MIN_US 240000L
#define FIRST_WAIT_SPAN_US 120000L
#define TIMER_WAIT_MIN_US 180000L
#define TIMER_WAIT_SPAN_US 520000L
#define EXTERNAL_WAIT_MIN_US 160000L
#define EXTERNAL_WAIT_SPAN_US 500000L
#define LONG_WAIT_BONUS_US 360000L
#define CPU_MIN_US 3000L
#define CPU_SPAN_US 9000L
#define PRODUCER_GAP_US 70000L
#define MAX_ALLOWED_LATE_US 2000000ull

enum runtime_wait_kind {
	RUNTIME_WAIT_TIMER = 0,
	RUNTIME_WAIT_EXTERNAL,
};

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static struct timespec log_start;

struct allocation_stats {
	_Atomic size_t timer_wait_allocs;
	_Atomic size_t timer_wait_frees;
	_Atomic size_t external_wait_allocs;
	_Atomic size_t external_wait_frees;
};

struct timer_source_stats {
	_Atomic size_t next_id;
	_Atomic size_t created;
	_Atomic size_t destroyed;
	_Atomic size_t registered;
	_Atomic size_t expired;
	_Atomic size_t local_wakes;
	_Atomic size_t blocking_waits;
	_Atomic size_t timed_waits;
	_Atomic size_t blocking_timeouts;
	_Atomic size_t idle_waits;
	_Atomic size_t wake_interrupts;
	_Atomic size_t suspends;
	_Atomic size_t collects;
	_Atomic size_t wakes;
	_Atomic size_t drained;
	_Atomic int failed;
};

struct external_source_stats {
	_Atomic size_t registered;
	_Atomic size_t early_rejected;
	_Atomic size_t successful_wakes;
	_Atomic size_t duplicate_rejected;
	_Atomic size_t timed_waits;
	_Atomic size_t drained;
	_Atomic int failed;
};

struct runtime_state {
	_Atomic size_t spawned;
	_Atomic size_t started;
	_Atomic size_t iterations;
	_Atomic size_t yields;
	_Atomic size_t waits_started;
	_Atomic size_t waits_completed;
	_Atomic size_t finished;
	_Atomic int failed;
};

struct timer_wait {
	croutine_list_head node;
	croutine_wait_handle handle;
	struct allocation_stats *allocs;
	struct timespec deadline;
	size_t task_id;
	size_t wait_no;
	long requested_us;
	_Atomic int queued;
};

struct external_source;

struct external_wait {
	croutine_list_head node;
	croutine_wait_handle handle;
	struct allocation_stats *allocs;
	struct external_source *source;
	struct timespec deadline;
	size_t task_id;
	size_t wait_no;
	long requested_us;
	_Atomic int queued;
	_Atomic int early_attempted;
};

struct timer_source {
	croutine_main_event_source base;
	struct timer_source_stats *stats;
	struct croutine_worker *worker;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	croutine_list_head waits;
	size_t id;
	int woken;
};

struct external_source {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	croutine_list_head waits;
	struct external_source_stats *stats;
	int stopping;
};

struct runtime_task_arg {
	struct runtime_state *runtime;
	struct allocation_stats *allocs;
	struct external_source *external;
	size_t id;
	size_t repeat;
	uint32_t seed;
	_Atomic size_t runs;
	size_t timer_waits;
	size_t external_waits;
	size_t wakes;
	size_t result;
	uint64_t requested_wait_us;
	uint64_t elapsed_wait_us;
	uint64_t max_wait_us;
	uint64_t max_late_us;
	uint64_t cpu_work_us;
	uint64_t runtime_us;
};

struct producer_arg {
	croutine_scheduler *scheduler;
	struct runtime_task_arg *tasks;
	size_t first;
	size_t count;
	_Atomic int *failed;
};

static int timespec_cmp(const struct timespec *left,
						const struct timespec *right) {
	if (left->tv_sec < right->tv_sec)
		return -1;
	if (left->tv_sec > right->tv_sec)
		return 1;
	if (left->tv_nsec < right->tv_nsec)
		return -1;
	if (left->tv_nsec > right->tv_nsec)
		return 1;
	return 0;
}

static void add_microseconds(struct timespec *time, long microseconds) {
	time->tv_nsec += (microseconds % 1000000L) * 1000L;
	time->tv_sec += microseconds / 1000000L;
	while (time->tv_nsec >= 1000000000L) {
		time->tv_sec++;
		time->tv_nsec -= 1000000000L;
	}
}

static uint64_t timespec_diff_us(const struct timespec *end,
								 const struct timespec *start) {
	time_t sec;
	long nsec;

	sec = end->tv_sec - start->tv_sec;
	nsec = end->tv_nsec - start->tv_nsec;
	if (nsec < 0) {
		sec--;
		nsec += 1000000000L;
	}

	return (uint64_t)sec * 1000000ull + (uint64_t)nsec / 1000ull;
}

static void log_event(const char *format, ...) {
	struct timespec now;
	va_list args;

	clock_gettime(CLOCK_MONOTONIC, &now);
	pthread_mutex_lock(&log_lock);
	printf("[%9.3f ms] ", (double)timespec_diff_us(&now, &log_start) / 1000.0);
	va_start(args, format);
	vprintf(format, args);
	va_end(args);
	putchar('\n');
	fflush(stdout);
	pthread_mutex_unlock(&log_lock);
}

static void sleep_microseconds(long microseconds) {
	struct timespec pause = {
		.tv_sec = microseconds / 1000000L,
		.tv_nsec = (microseconds % 1000000L) * 1000L,
	};

	while (nanosleep(&pause, &pause) != 0 && errno == EINTR)
		;
}

static uint32_t next_random(uint32_t *seed) {
	*seed = *seed * 1664525u + 1013904223u;
	return *seed;
}

static uint32_t seed_from_env(void) {
	const char *text = getenv("CROUTINE_TEST_SEED");
	char *end;
	unsigned long parsed;

	if (text == NULL || *text == '\0')
		return 0xc001d00du;

	errno = 0;
	parsed = strtoul(text, &end, 0);
	if (errno != 0 || end == text || *end != '\0')
		return 0xc001d00du;
	return (uint32_t)parsed;
}

static void burn_for_microseconds(long target_us, uint32_t seed) {
	struct timespec start;
	struct timespec now;
	volatile uint64_t value = seed + 0x9e3779b97f4a7c15ull;

	clock_gettime(CLOCK_MONOTONIC, &start);
	do {
		for (size_t index = 0; index < 2048; index++)
			value = value * 2862933555777941757ull + index + 3037000493ull;
		clock_gettime(CLOCK_MONOTONIC, &now);
	} while (timespec_diff_us(&now, &start) < (uint64_t)target_us);
	(void)value;
}

static struct timer_wait *timer_wait_alloc(struct allocation_stats *allocs) {
	struct timer_wait *wait = calloc(1, sizeof(*wait));

	if (wait != NULL) {
		wait->allocs = allocs;
		atomic_fetch_add_explicit(&allocs->timer_wait_allocs, 1,
								  memory_order_acq_rel);
	}
	return wait;
}

static void timer_wait_free(struct timer_wait *wait) {
	if (wait == NULL)
		return;

	atomic_fetch_add_explicit(&wait->allocs->timer_wait_frees, 1,
							  memory_order_acq_rel);
	free(wait);
}

static struct external_wait *
external_wait_alloc(struct allocation_stats *allocs) {
	struct external_wait *wait = calloc(1, sizeof(*wait));

	if (wait != NULL) {
		wait->allocs = allocs;
		atomic_fetch_add_explicit(&allocs->external_wait_allocs, 1,
								  memory_order_acq_rel);
	}
	return wait;
}

static void external_wait_free(struct external_wait *wait) {
	if (wait == NULL)
		return;

	atomic_fetch_add_explicit(&wait->allocs->external_wait_frees, 1,
							  memory_order_acq_rel);
	free(wait);
}

static int external_wait_put(struct external_wait *wait) {
	int ret;

	ret = croutine_wait_handle_release(&wait->handle);
	if (ret == 1)
		external_wait_free(wait);
	return ret;
}

static int timer_deadline_checker(void *data) {
	const struct timer_wait *wait = data;
	struct timespec now;

	if (wait == NULL)
		return 0;

	clock_gettime(CLOCK_REALTIME, &now);
	return timespec_cmp(&now, &wait->deadline) >= 0;
}

static int external_deadline_checker(void *data) {
	const struct external_wait *wait = data;
	struct timespec now;

	if (wait == NULL)
		return 0;

	clock_gettime(CLOCK_REALTIME, &now);
	return timespec_cmp(&now, &wait->deadline) >= 0;
}

static struct timer_source *
timer_source_from_base(croutine_main_event_source *source) {
	return (struct timer_source *)source;
}

static void timer_source_insert_wait(struct timer_source *timer,
									 struct timer_wait *wait) {
	croutine_list_head *pos;

	croutine_list_for_each(pos, &timer->waits) {
		struct timer_wait *current;

		current = croutine_list_entry(pos, struct timer_wait, node);
		if (timespec_cmp(&wait->deadline, &current->deadline) < 0) {
			croutine_list_insert_before(pos, &wait->node);
			atomic_store_explicit(&wait->queued, 1, memory_order_release);
			return;
		}
	}

	croutine_list_push_back(&timer->waits, &wait->node);
	atomic_store_explicit(&wait->queued, 1, memory_order_release);
}

static int timer_source_add_wait(struct timer_source *timer,
								 struct timer_wait *wait) {
	if (timer == NULL || wait == NULL)
		return -1;

	pthread_mutex_lock(&timer->lock);
	timer_source_insert_wait(timer, wait);
	atomic_fetch_add_explicit(&timer->stats->registered, 1,
							  memory_order_acq_rel);
	timer->woken = 1;
	pthread_cond_signal(&timer->cond);
	pthread_mutex_unlock(&timer->lock);
	return 0;
}

static enum croutine_main_event_wait_result
timer_source_blocking_wait(croutine_main_event_source *source) {
	struct timer_source *timer = timer_source_from_base(source);
	int ret = 0;

	atomic_fetch_add_explicit(&timer->stats->blocking_waits, 1,
							  memory_order_acq_rel);

	pthread_mutex_lock(&timer->lock);
	if (timer->woken) {
		timer->woken = 0;
		atomic_fetch_add_explicit(&timer->stats->wake_interrupts, 1,
								  memory_order_acq_rel);
		log_event("timer %zu blocking_wait interrupted by wake", timer->id);
		pthread_mutex_unlock(&timer->lock);
		return CROUTINE_MAIN_EVENT_WAIT_DONE;
	}

	if (croutine_list_empty(&timer->waits)) {
		atomic_fetch_add_explicit(&timer->stats->idle_waits, 1,
								  memory_order_acq_rel);
		log_event("timer %zu blocking_wait idle", timer->id);
		pthread_mutex_unlock(&timer->lock);
		return CROUTINE_MAIN_EVENT_WAIT_EMPTY;
	} else {
		struct timer_wait *wait;
		struct timespec now;
		uint64_t wait_us;

		wait = croutine_list_entry(timer->waits.next, struct timer_wait, node);
		clock_gettime(CLOCK_REALTIME, &now);
		wait_us = timespec_cmp(&wait->deadline, &now) > 0 ?
					  timespec_diff_us(&wait->deadline, &now) :
					  0;
		atomic_fetch_add_explicit(&timer->stats->timed_waits, 1,
								  memory_order_acq_rel);
		log_event("timer %zu blocking_wait task %zu wait %zu in %.2fms",
				  timer->id, wait->task_id, wait->wait_no,
				  (double)wait_us / 1000.0);
		ret =
			pthread_cond_timedwait(&timer->cond, &timer->lock, &wait->deadline);
	}

	if (ret == ETIMEDOUT) {
		atomic_fetch_add_explicit(&timer->stats->blocking_timeouts, 1,
								  memory_order_acq_rel);
		log_event("timer %zu blocking_wait deadline reached", timer->id);
	} else if (ret == 0) {
		atomic_fetch_add_explicit(&timer->stats->wake_interrupts, 1,
								  memory_order_acq_rel);
		log_event("timer %zu blocking_wait signaled", timer->id);
	} else {
		atomic_store_explicit(&timer->stats->failed, 1, memory_order_release);
		pthread_mutex_unlock(&timer->lock);
		return CROUTINE_MAIN_EVENT_WAIT_ERROR;
	}
	timer->woken = 0;
	pthread_mutex_unlock(&timer->lock);
	return CROUTINE_MAIN_EVENT_WAIT_DONE;
}

static void timer_source_collect(croutine_main_event_source *source) {
	struct timer_source *timer = timer_source_from_base(source);
	struct timespec now;

	atomic_fetch_add_explicit(&timer->stats->collects, 1, memory_order_acq_rel);
	clock_gettime(CLOCK_REALTIME, &now);

	pthread_mutex_lock(&timer->lock);
	while (!croutine_list_empty(&timer->waits)) {
		struct timer_wait *wait;
		uint64_t late_us;
		size_t task_id;
		size_t wait_no;

		wait = croutine_list_entry(timer->waits.next, struct timer_wait, node);
		if (timespec_cmp(&wait->deadline, &now) > 0)
			break;

		task_id = wait->task_id;
		wait_no = wait->wait_no;
		late_us = timespec_diff_us(&now, &wait->deadline);
		croutine_list_remove(&wait->node);
		atomic_store_explicit(&wait->queued, 0, memory_order_release);
		atomic_fetch_add_explicit(&timer->stats->expired, 1,
								  memory_order_acq_rel);
		log_event("timer %zu collect task %zu wait %zu late %.2fms", timer->id,
				  task_id, wait_no, (double)late_us / 1000.0);
		if (croutine_wait_handle_wake(&wait->handle) != 0) {
			fprintf(stderr, "timer %zu failed to wake task %zu wait %zu\n",
					timer->id, task_id, wait_no);
			atomic_store_explicit(&timer->stats->failed, 1,
								  memory_order_release);
		} else {
			atomic_fetch_add_explicit(&timer->stats->local_wakes, 1,
									  memory_order_acq_rel);
		}
	}
	pthread_mutex_unlock(&timer->lock);
}

static int timer_source_wake(croutine_main_event_source *source) {
	struct timer_source *timer = timer_source_from_base(source);

	atomic_fetch_add_explicit(&timer->stats->wakes, 1, memory_order_acq_rel);
	pthread_mutex_lock(&timer->lock);
	timer->woken = 1;
	pthread_cond_signal(&timer->cond);
	pthread_mutex_unlock(&timer->lock);
	return 0;
}

static void timer_source_suspend(croutine_main_event_source *source) {
	struct timer_source *timer = timer_source_from_base(source);

	atomic_fetch_add_explicit(&timer->stats->suspends, 1, memory_order_acq_rel);
	pthread_mutex_lock(&timer->lock);
	while (!timer->woken)
		(void)pthread_cond_wait(&timer->cond, &timer->lock);
	timer->woken = 0;
	pthread_mutex_unlock(&timer->lock);
}

static void timer_source_destroy(croutine_main_event_source *source) {
	struct timer_source *timer = timer_source_from_base(source);
	struct croutine_list_head *pos;
	struct croutine_list_head *tmp;

	pthread_mutex_lock(&timer->lock);
	croutine_list_for_each_safe(pos, tmp, &timer->waits) {
		struct timer_wait *wait;

		wait = croutine_list_entry(pos, struct timer_wait, node);
		croutine_list_remove(&wait->node);
		atomic_store_explicit(&wait->queued, 0, memory_order_release);
		atomic_fetch_add_explicit(&timer->stats->drained, 1,
								  memory_order_acq_rel);
		timer_wait_free(wait);
	}
	pthread_mutex_unlock(&timer->lock);

	atomic_fetch_add_explicit(&timer->stats->destroyed, 1,
							  memory_order_acq_rel);
	log_event("timer %zu destroy", timer->id);
	pthread_cond_destroy(&timer->cond);
	pthread_mutex_destroy(&timer->lock);
	free(timer);
}

static croutine_main_event_source *timer_source_factory(croutine_worker *worker,
														void *args) {
	struct timer_source_stats *stats = args;
	struct timer_source *timer;

	timer = calloc(1, sizeof(*timer));
	if (timer == NULL)
		return NULL;

	timer->stats = stats;
	timer->worker = worker;
	timer->id =
		atomic_fetch_add_explicit(&stats->next_id, 1, memory_order_acq_rel);
	timer->base.blocking_wait = timer_source_blocking_wait;
	timer->base.collect = timer_source_collect;
	timer->base.wake = timer_source_wake;
	timer->base.suspend = timer_source_suspend;
	timer->base.destroy = timer_source_destroy;
	croutine_list_init(&timer->waits);

	if (pthread_mutex_init(&timer->lock, NULL) != 0)
		goto fail;
	if (pthread_cond_init(&timer->cond, NULL) != 0)
		goto fail_lock;

	atomic_fetch_add_explicit(&stats->created, 1, memory_order_acq_rel);
	log_event("timer %zu created", timer->id);
	return &timer->base;

fail_lock:
	pthread_mutex_destroy(&timer->lock);
fail:
	free(timer);
	return NULL;
}

static void external_source_insert_wait(struct external_source *source,
										struct external_wait *wait) {
	croutine_list_head *pos;

	croutine_list_for_each(pos, &source->waits) {
		struct external_wait *current;

		current = croutine_list_entry(pos, struct external_wait, node);
		if (timespec_cmp(&wait->deadline, &current->deadline) < 0) {
			croutine_list_insert_before(pos, &wait->node);
			atomic_store_explicit(&wait->queued, 1, memory_order_release);
			return;
		}
	}

	croutine_list_push_back(&source->waits, &wait->node);
	atomic_store_explicit(&wait->queued, 1, memory_order_release);
}

static int external_source_add_wait(struct external_source *source,
									struct external_wait *wait) {
	if (source == NULL || wait == NULL)
		return -1;

	pthread_mutex_lock(&source->lock);
	external_source_insert_wait(source, wait);
	atomic_fetch_add_explicit(&source->stats->registered, 1,
							  memory_order_acq_rel);
	pthread_cond_signal(&source->cond);
	pthread_mutex_unlock(&source->lock);
	return 0;
}

static int external_source_init(struct external_source *source,
								struct external_source_stats *stats) {
	source->stats = stats;
	source->stopping = 0;
	croutine_list_init(&source->waits);
	if (pthread_mutex_init(&source->lock, NULL) != 0)
		return -1;
	if (pthread_cond_init(&source->cond, NULL) != 0) {
		pthread_mutex_destroy(&source->lock);
		return -1;
	}
	return 0;
}

static void external_source_stop(struct external_source *source) {
	pthread_mutex_lock(&source->lock);
	source->stopping = 1;
	pthread_cond_broadcast(&source->cond);
	pthread_mutex_unlock(&source->lock);
}

static void external_source_destroy(struct external_source *source) {
	struct croutine_list_head *pos;
	struct croutine_list_head *tmp;

	pthread_mutex_lock(&source->lock);
	croutine_list_for_each_safe(pos, tmp, &source->waits) {
		struct external_wait *wait;

		wait = croutine_list_entry(pos, struct external_wait, node);
		croutine_list_remove(&wait->node);
		atomic_store_explicit(&wait->queued, 0, memory_order_release);
		atomic_fetch_add_explicit(&source->stats->drained, 1,
								  memory_order_acq_rel);
		(void)external_wait_put(wait);
		(void)external_wait_put(wait);
	}
	pthread_mutex_unlock(&source->lock);

	pthread_cond_destroy(&source->cond);
	pthread_mutex_destroy(&source->lock);
}

static void *external_source_main(void *arg) {
	struct external_source *source = arg;

	for (;;) {
		struct external_wait *wait;
		struct timespec now;
		int ret;

		pthread_mutex_lock(&source->lock);
		while (croutine_list_empty(&source->waits) && !source->stopping)
			(void)pthread_cond_wait(&source->cond, &source->lock);
		if (source->stopping && croutine_list_empty(&source->waits)) {
			pthread_mutex_unlock(&source->lock);
			return NULL;
		}

		wait =
			croutine_list_entry(source->waits.next, struct external_wait, node);
		if (!atomic_load_explicit(&wait->early_attempted,
								  memory_order_acquire)) {
			atomic_store_explicit(&wait->early_attempted, 1,
								  memory_order_release);
			pthread_mutex_unlock(&source->lock);
			ret = croutine_wait_handle_wake(&wait->handle);
			if (ret == -1) {
				atomic_fetch_add_explicit(&source->stats->early_rejected, 1,
										  memory_order_acq_rel);
				log_event("external early wake rejected task %zu wait %zu",
						  wait->task_id, wait->wait_no);
			} else {
				fprintf(stderr,
						"external early wake unexpectedly succeeded task %zu\n",
						wait->task_id);
				atomic_store_explicit(&source->stats->failed, 1,
									  memory_order_release);
			}
			continue;
		}

		clock_gettime(CLOCK_REALTIME, &now);
		if (timespec_cmp(&wait->deadline, &now) > 0) {
			atomic_fetch_add_explicit(&source->stats->timed_waits, 1,
									  memory_order_acq_rel);
			ret = pthread_cond_timedwait(&source->cond, &source->lock,
										 &wait->deadline);
			pthread_mutex_unlock(&source->lock);
			if (ret != 0 && ret != ETIMEDOUT)
				atomic_store_explicit(&source->stats->failed, 1,
									  memory_order_release);
			continue;
		}

		croutine_list_remove(&wait->node);
		atomic_store_explicit(&wait->queued, 0, memory_order_release);
		pthread_mutex_unlock(&source->lock);

		ret = croutine_wait_handle_wake(&wait->handle);
		if (ret == 0) {
			atomic_fetch_add_explicit(&source->stats->successful_wakes, 1,
									  memory_order_acq_rel);
			log_event("external wake task %zu wait %zu", wait->task_id,
					  wait->wait_no);
		} else {
			fprintf(stderr, "external wake failed task %zu wait %zu\n",
					wait->task_id, wait->wait_no);
			atomic_store_explicit(&source->stats->failed, 1,
								  memory_order_release);
		}

		ret = croutine_wait_handle_wake(&wait->handle);
		if (ret == -1) {
			atomic_fetch_add_explicit(&source->stats->duplicate_rejected, 1,
									  memory_order_acq_rel);
		} else {
			fprintf(stderr, "external duplicate wake unexpectedly succeeded\n");
			atomic_store_explicit(&source->stats->failed, 1,
								  memory_order_release);
		}

		if (external_wait_put(wait) < 0)
			atomic_store_explicit(&source->stats->failed, 1,
								  memory_order_release);
	}
}

static void runtime_task_fail(struct runtime_task_arg *task,
							  const char *message) {
	fprintf(stderr, "task %zu: %s\n", task->id, message);
	atomic_store_explicit(&task->runtime->failed, 1, memory_order_release);
}

static void update_wait_metrics(struct runtime_task_arg *task,
								long requested_us, const struct timespec *start,
								const struct timespec *deadline,
								const struct timespec *now) {
	uint64_t elapsed_us;
	uint64_t late_us;

	elapsed_us = timespec_diff_us(now, start);
	late_us = timespec_diff_us(now, deadline);
	task->wakes++;
	task->requested_wait_us += (uint64_t)requested_us;
	task->elapsed_wait_us += elapsed_us;
	if (elapsed_us > task->max_wait_us)
		task->max_wait_us = elapsed_us;
	if (late_us > task->max_late_us)
		task->max_late_us = late_us;
}

static int runtime_timer_wait(struct runtime_task_arg *task,
							  long delay_microseconds, size_t wait_no) {
	struct timer_source *timer;
	struct timer_wait *wait;
	struct timespec start;
	struct timespec now;
	struct croutine_task *current;

	current = croutine_task_current();
	if (current == NULL || current->worker == NULL ||
		current->worker->main_event_source == NULL)
		return -1;

	timer = timer_source_from_base(current->worker->main_event_source);
	wait = timer_wait_alloc(task->allocs);
	if (wait == NULL)
		return -1;

	croutine_list_init(&wait->node);
	wait->task_id = task->id;
	wait->wait_no = wait_no;
	wait->requested_us = delay_microseconds;
	atomic_init(&wait->queued, 0);
	clock_gettime(CLOCK_REALTIME, &start);
	wait->deadline = start;
	add_microseconds(&wait->deadline, delay_microseconds);
	if (croutine_wait_handle_init_default(&wait->handle, current, wait,
										  timer_deadline_checker) != 0) {
		timer_wait_free(wait);
		return -1;
	}

	log_event("task %zu wait %zu TIMER %.2fms on timer %zu", task->id, wait_no,
			  (double)delay_microseconds / 1000.0, timer->id);
	if (timer_source_add_wait(timer, wait) != 0) {
		timer_wait_free(wait);
		return -1;
	}

	task->timer_waits++;
	atomic_fetch_add_explicit(&task->runtime->waits_started, 1,
							  memory_order_acq_rel);
	croutine_task_wait();

	clock_gettime(CLOCK_REALTIME, &now);
	if (timespec_cmp(&now, &wait->deadline) < 0) {
		timer_wait_free(wait);
		runtime_task_fail(task, "timer woke before deadline");
		return -1;
	}
	if (atomic_load_explicit(&wait->queued, memory_order_acquire) != 0) {
		timer_wait_free(wait);
		runtime_task_fail(task, "timer wait remained queued");
		return -1;
	}
	if (atomic_load_explicit(&wait->handle.state, memory_order_acquire) !=
		CROUTINE_WAIT_HANDLE_FINISHED) {
		timer_wait_free(wait);
		runtime_task_fail(task, "timer wait handle did not finish");
		return -1;
	}

	update_wait_metrics(task, delay_microseconds, &start, &wait->deadline,
						&now);
	log_event("task %zu wait %zu TIMER ready elapsed %.2fms late %.2fms",
			  task->id, wait_no,
			  (double)timespec_diff_us(&now, &start) / 1000.0,
			  (double)timespec_diff_us(&now, &wait->deadline) / 1000.0);
	timer_wait_free(wait);
	atomic_fetch_add_explicit(&task->runtime->waits_completed, 1,
							  memory_order_acq_rel);
	return 0;
}

static int runtime_external_wait(struct runtime_task_arg *task,
								 long delay_microseconds, size_t wait_no) {
	struct external_wait *wait;
	struct timespec start;
	struct timespec now;
	struct croutine_task *current;

	current = croutine_task_current();
	if (current == NULL)
		return -1;

	wait = external_wait_alloc(task->allocs);
	if (wait == NULL)
		return -1;

	croutine_list_init(&wait->node);
	wait->source = task->external;
	wait->task_id = task->id;
	wait->wait_no = wait_no;
	wait->requested_us = delay_microseconds;
	atomic_init(&wait->queued, 0);
	atomic_init(&wait->early_attempted, 0);
	clock_gettime(CLOCK_REALTIME, &start);
	wait->deadline = start;
	add_microseconds(&wait->deadline, delay_microseconds);
	if (croutine_wait_handle_init_complex(&wait->handle, current, 2, wait,
										  external_deadline_checker) != 0) {
		external_wait_free(wait);
		return -1;
	}

	log_event("task %zu wait %zu EXTERNAL %.2fms", task->id, wait_no,
			  (double)delay_microseconds / 1000.0);
	if (external_source_add_wait(task->external, wait) != 0) {
		(void)external_wait_put(wait);
		(void)external_wait_put(wait);
		return -1;
	}

	task->external_waits++;
	atomic_fetch_add_explicit(&task->runtime->waits_started, 1,
							  memory_order_acq_rel);
	croutine_task_wait();

	clock_gettime(CLOCK_REALTIME, &now);
	if (timespec_cmp(&now, &wait->deadline) < 0) {
		(void)external_wait_put(wait);
		runtime_task_fail(task, "external wait woke before deadline");
		return -1;
	}
	if (atomic_load_explicit(&wait->queued, memory_order_acquire) != 0) {
		(void)external_wait_put(wait);
		runtime_task_fail(task, "external wait remained queued");
		return -1;
	}
	if (!atomic_load_explicit(&wait->early_attempted, memory_order_acquire)) {
		(void)external_wait_put(wait);
		runtime_task_fail(task, "external checker path was not exercised");
		return -1;
	}
	if (atomic_load_explicit(&wait->handle.state, memory_order_acquire) !=
		CROUTINE_WAIT_HANDLE_FINISHED) {
		(void)external_wait_put(wait);
		runtime_task_fail(task, "external wait handle did not finish");
		return -1;
	}

	update_wait_metrics(task, delay_microseconds, &start, &wait->deadline,
						&now);
	log_event("task %zu wait %zu EXTERNAL ready elapsed %.2fms late %.2fms",
			  task->id, wait_no,
			  (double)timespec_diff_us(&now, &start) / 1000.0,
			  (double)timespec_diff_us(&now, &wait->deadline) / 1000.0);
	if (external_wait_put(wait) < 0) {
		runtime_task_fail(task, "external wait release failed");
		return -1;
	}
	atomic_fetch_add_explicit(&task->runtime->waits_completed, 1,
							  memory_order_acq_rel);
	return 0;
}

static enum runtime_wait_kind
choose_wait_kind(const struct runtime_task_arg *task, size_t index,
				 uint32_t random) {
	if (task->id < INITIAL_TASKS && index == 0)
		return RUNTIME_WAIT_TIMER;
	if (((random >> 5) + task->id + index) % 3 == 0)
		return RUNTIME_WAIT_EXTERNAL;
	return RUNTIME_WAIT_TIMER;
}

static long choose_wait_delay(const struct runtime_task_arg *task,
							  enum runtime_wait_kind kind, size_t index,
							  uint32_t random) {
	long delay;

	if (task->id < INITIAL_TASKS && index == 0)
		return FIRST_WAIT_MIN_US + (long)(random % FIRST_WAIT_SPAN_US);

	if (kind == RUNTIME_WAIT_EXTERNAL)
		delay = EXTERNAL_WAIT_MIN_US + (long)(random % EXTERNAL_WAIT_SPAN_US);
	else
		delay = TIMER_WAIT_MIN_US + (long)(random % TIMER_WAIT_SPAN_US);
	if (((task->id + index + (random >> 17)) % 6) == 0)
		delay += LONG_WAIT_BONUS_US;
	return delay;
}

static void *runtime_task(void *arg) {
	struct runtime_task_arg *task = arg;
	struct runtime_state *runtime = task->runtime;
	struct timespec task_start;
	struct timespec task_end;

	if (croutine_task_current() == NULL ||
		croutine_scheduler_current() == NULL) {
		runtime_task_fail(task, "missing runtime TLS");
		return NULL;
	}

	clock_gettime(CLOCK_MONOTONIC, &task_start);
	log_event("task %zu start repeat=%zu seed=0x%08x", task->id, task->repeat,
			  task->seed);
	atomic_fetch_add_explicit(&runtime->started, 1, memory_order_acq_rel);
	for (size_t index = 0; index < task->repeat; index++) {
		struct timespec cpu_start;
		struct timespec cpu_end;
		enum runtime_wait_kind kind;
		uint32_t random;
		long cpu_target;
		long wait_time;
		uint64_t cpu_us;
		size_t runs;

		if (croutine_task_current() == NULL) {
			runtime_task_fail(task, "lost current task TLS");
			return NULL;
		}

		random = next_random(&task->seed);
		cpu_target = CPU_MIN_US + (long)(random % CPU_SPAN_US);
		log_event("task %zu iter %zu/%zu CPU target %.2fms", task->id,
				  index + 1, task->repeat, (double)cpu_target / 1000.0);
		clock_gettime(CLOCK_MONOTONIC, &cpu_start);
		burn_for_microseconds(cpu_target, random ^ (uint32_t)task->id);
		clock_gettime(CLOCK_MONOTONIC, &cpu_end);
		cpu_us = timespec_diff_us(&cpu_end, &cpu_start);
		task->cpu_work_us += cpu_us;

		runs =
			atomic_fetch_add_explicit(&task->runs, 1, memory_order_acq_rel) + 1;
		atomic_fetch_add_explicit(&runtime->iterations, 1,
								  memory_order_acq_rel);
		log_event("task %zu iter %zu/%zu CPU actual %.2fms", task->id,
				  index + 1, task->repeat, (double)cpu_us / 1000.0);

		kind = choose_wait_kind(task, index, random);
		wait_time = choose_wait_delay(task, kind, index, random);
		if (kind == RUNTIME_WAIT_EXTERNAL) {
			if (runtime_external_wait(task, wait_time, runs) != 0)
				return NULL;
		} else {
			if (runtime_timer_wait(task, wait_time, runs) != 0)
				return NULL;
		}

		if (((random >> 21) & 3u) == 0) {
			log_event("task %zu iter %zu/%zu voluntary yield", task->id,
					  index + 1, task->repeat);
			atomic_fetch_add_explicit(&runtime->yields, 1,
									  memory_order_acq_rel);
			croutine_yield();
			log_event("task %zu iter %zu/%zu yield returned", task->id,
					  index + 1, task->repeat);
		}
	}

	task->result = task->id * 4099 + task->repeat;
	clock_gettime(CLOCK_MONOTONIC, &task_end);
	task->runtime_us = timespec_diff_us(&task_end, &task_start);
	atomic_fetch_add_explicit(&runtime->finished, 1, memory_order_acq_rel);
	log_event(
		"task %zu finish timer=%zu external=%zu runtime=%.2fms result=%zu",
		task->id, task->timer_waits, task->external_waits,
		(double)task->runtime_us / 1000.0, task->result);
	return &task->result;
}

static void init_runtime_task(struct runtime_task_arg *task,
							  struct runtime_state *runtime,
							  struct allocation_stats *allocs,
							  struct external_source *external, size_t id,
							  size_t repeat, uint32_t seed) {
	task->runtime = runtime;
	task->allocs = allocs;
	task->external = external;
	task->id = id;
	task->repeat = repeat;
	task->seed = seed ^ (uint32_t)(id * 2654435761u);
	atomic_init(&task->runs, 0);
	task->timer_waits = 0;
	task->external_waits = 0;
	task->wakes = 0;
	task->result = 0;
	task->requested_wait_us = 0;
	task->elapsed_wait_us = 0;
	task->max_wait_us = 0;
	task->max_late_us = 0;
	task->cpu_work_us = 0;
	task->runtime_us = 0;
}

static void *producer_main(void *arg) {
	struct producer_arg *producer = arg;

	for (size_t index = 0; index < producer->count; index++) {
		struct runtime_task_arg *task =
			&producer->tasks[producer->first + index];

		log_event("producer spawn task %zu", task->id);
		if (croutine_spawn(producer->scheduler, runtime_task, task) != 0) {
			fprintf(stderr, "producer failed to spawn task %zu\n", task->id);
			atomic_store_explicit(producer->failed, 1, memory_order_release);
			return NULL;
		}

		atomic_fetch_add_explicit(&task->runtime->spawned, 1,
								  memory_order_acq_rel);
		sleep_microseconds(PRODUCER_GAP_US + (long)(task->id % 3) * 30000L);
	}

	return NULL;
}

static int
wait_for_at_least(_Atomic size_t *value, size_t target,
				  const struct runtime_state *runtime,
				  const struct timer_source_stats *timer_stats,
				  const struct external_source_stats *external_stats) {
	for (size_t round = 0; round < WAIT_ROUNDS; round++) {
		if (atomic_load_explicit(&runtime->failed, memory_order_acquire) != 0 ||
			atomic_load_explicit(&timer_stats->failed, memory_order_acquire) !=
				0 ||
			atomic_load_explicit(&external_stats->failed,
								 memory_order_acquire) != 0)
			return -1;
		if (atomic_load_explicit(value, memory_order_acquire) >= target)
			return 0;
		sleep_microseconds(1000);
	}

	return -1;
}

static int
wait_for_workers_suspended(const croutine_scheduler *scheduler,
						   const struct runtime_state *runtime,
						   const struct timer_source_stats *timer_stats,
						   const struct external_source_stats *external_stats) {
	for (size_t round = 0; round < WAIT_ROUNDS; round++) {
		size_t suspended = 0;

		if (atomic_load_explicit(&runtime->failed, memory_order_acquire) != 0 ||
			atomic_load_explicit(&timer_stats->failed, memory_order_acquire) !=
				0 ||
			atomic_load_explicit(&external_stats->failed,
								 memory_order_acquire) != 0)
			return -1;

		for (size_t index = 0; index < scheduler->worker_count; index++) {
			if (atomic_load_explicit(&scheduler->workers[index].state,
									 memory_order_acquire) ==
				CROUTINE_WORKER_SUSPENDED)
				suspended++;
		}
		if (suspended == scheduler->worker_count)
			return 0;
		sleep_microseconds(1000);
	}

	return -1;
}

static size_t expected_iterations(const struct runtime_task_arg *tasks) {
	size_t expected = 0;

	for (size_t index = 0; index < TOTAL_TASKS; index++)
		expected += tasks[index].repeat;
	return expected;
}

static void sum_task_waits(const struct runtime_task_arg *tasks,
						   size_t *timer_waits, size_t *external_waits) {
	*timer_waits = 0;
	*external_waits = 0;
	for (size_t index = 0; index < TOTAL_TASKS; index++) {
		*timer_waits += tasks[index].timer_waits;
		*external_waits += tasks[index].external_waits;
	}
}

static int verify_tasks(const struct runtime_task_arg *tasks,
						size_t total_iterations) {
	size_t expected = 0;

	for (size_t index = 0; index < TOTAL_TASKS; index++) {
		size_t runs;

		runs = atomic_load_explicit(&tasks[index].runs, memory_order_acquire);
		expected += tasks[index].repeat;
		if (runs != tasks[index].repeat) {
			fprintf(stderr, "task %zu ran %zu/%zu iterations\n",
					tasks[index].id, runs, tasks[index].repeat);
			return -1;
		}
		if (tasks[index].wakes != tasks[index].repeat) {
			fprintf(stderr, "task %zu woke %zu/%zu times\n", tasks[index].id,
					tasks[index].wakes, tasks[index].repeat);
			return -1;
		}
		if (tasks[index].elapsed_wait_us < tasks[index].requested_wait_us) {
			fprintf(stderr, "task %zu elapsed wait shorter than requested\n",
					tasks[index].id);
			return -1;
		}
		if (tasks[index].max_late_us > MAX_ALLOWED_LATE_US) {
			fprintf(stderr, "task %zu late wait exceeded limit\n",
					tasks[index].id);
			return -1;
		}
	}

	if (total_iterations != expected) {
		fprintf(stderr, "runtime iteration count %zu/%zu\n", total_iterations,
				expected);
		return -1;
	}

	return 0;
}

static int
verify_stopped_deadline_resume(const struct runtime_task_arg *tasks) {
	for (size_t index = 0; index < INITIAL_TASKS; index++) {
		if (tasks[index].max_late_us >= (uint64_t)STOP_PAUSE_US / 3)
			return 0;
	}

	fprintf(stderr, "stopped timer deadlines were not observed after resume\n");
	return -1;
}

static int verify_allocations(const struct allocation_stats *allocs) {
	size_t timer_allocs;
	size_t timer_frees;
	size_t external_allocs;
	size_t external_frees;

	timer_allocs =
		atomic_load_explicit(&allocs->timer_wait_allocs, memory_order_acquire);
	timer_frees =
		atomic_load_explicit(&allocs->timer_wait_frees, memory_order_acquire);
	external_allocs = atomic_load_explicit(&allocs->external_wait_allocs,
										   memory_order_acquire);
	external_frees = atomic_load_explicit(&allocs->external_wait_frees,
										  memory_order_acquire);

	if (timer_allocs != timer_frees) {
		fprintf(stderr, "timer wait allocation leak: alloc=%zu free=%zu\n",
				timer_allocs, timer_frees);
		return -1;
	}
	if (external_allocs != external_frees) {
		fprintf(stderr, "external wait allocation leak: alloc=%zu free=%zu\n",
				external_allocs, external_frees);
		return -1;
	}
	return 0;
}

static void
print_runtime_report(const struct runtime_task_arg *tasks,
					 const struct runtime_state *runtime,
					 const struct timer_source_stats *timer_stats,
					 const struct external_source_stats *external_stats,
					 const struct allocation_stats *allocs, size_t expected) {
	uint64_t total_requested = 0;
	uint64_t total_elapsed = 0;
	uint64_t total_cpu = 0;
	uint64_t total_runtime = 0;
	size_t timer_waits;
	size_t external_waits;

	sum_task_waits(tasks, &timer_waits, &external_waits);
	for (size_t index = 0; index < TOTAL_TASKS; index++) {
		total_requested += tasks[index].requested_wait_us;
		total_elapsed += tasks[index].elapsed_wait_us;
		total_cpu += tasks[index].cpu_work_us;
		total_runtime += tasks[index].runtime_us;
	}

	printf("\nruntime summary\n");
	printf("  workers=%d tasks=%d seed=0x%08x expected_waits=%zu\n",
		   RUNTIME_WORKERS, TOTAL_TASKS, seed_from_env(), expected);
	printf("  spawned=%zu started=%zu finished=%zu iterations=%zu yields=%zu\n",
		   atomic_load_explicit(&runtime->spawned, memory_order_acquire),
		   atomic_load_explicit(&runtime->started, memory_order_acquire),
		   atomic_load_explicit(&runtime->finished, memory_order_acquire),
		   atomic_load_explicit(&runtime->iterations, memory_order_acquire),
		   atomic_load_explicit(&runtime->yields, memory_order_acquire));
	printf(
		"  waits: started=%zu completed=%zu timer=%zu external=%zu\n",
		atomic_load_explicit(&runtime->waits_started, memory_order_acquire),
		atomic_load_explicit(&runtime->waits_completed, memory_order_acquire),
		timer_waits, external_waits);
	printf(
		"  timer: created=%zu destroyed=%zu registered=%zu expired=%zu local_wakes=%zu drained=%zu\n",
		atomic_load_explicit(&timer_stats->created, memory_order_acquire),
		atomic_load_explicit(&timer_stats->destroyed, memory_order_acquire),
		atomic_load_explicit(&timer_stats->registered, memory_order_acquire),
		atomic_load_explicit(&timer_stats->expired, memory_order_acquire),
		atomic_load_explicit(&timer_stats->local_wakes, memory_order_acquire),
		atomic_load_explicit(&timer_stats->drained, memory_order_acquire));
	printf(
		"  timer paths: blocking=%zu timed=%zu timeouts=%zu idle=%zu interrupts=%zu suspends=%zu collects=%zu wakes=%zu\n",
		atomic_load_explicit(&timer_stats->blocking_waits,
							 memory_order_acquire),
		atomic_load_explicit(&timer_stats->timed_waits, memory_order_acquire),
		atomic_load_explicit(&timer_stats->blocking_timeouts,
							 memory_order_acquire),
		atomic_load_explicit(&timer_stats->idle_waits, memory_order_acquire),
		atomic_load_explicit(&timer_stats->wake_interrupts,
							 memory_order_acquire),
		atomic_load_explicit(&timer_stats->suspends, memory_order_acquire),
		atomic_load_explicit(&timer_stats->collects, memory_order_acquire),
		atomic_load_explicit(&timer_stats->wakes, memory_order_acquire));
	printf(
		"  external: registered=%zu early_rejected=%zu success=%zu duplicate_rejected=%zu timed_waits=%zu drained=%zu\n",
		atomic_load_explicit(&external_stats->registered, memory_order_acquire),
		atomic_load_explicit(&external_stats->early_rejected,
							 memory_order_acquire),
		atomic_load_explicit(&external_stats->successful_wakes,
							 memory_order_acquire),
		atomic_load_explicit(&external_stats->duplicate_rejected,
							 memory_order_acquire),
		atomic_load_explicit(&external_stats->timed_waits,
							 memory_order_acquire),
		atomic_load_explicit(&external_stats->drained, memory_order_acquire));
	printf(
		"  allocations: timer=%zu/%zu external=%zu/%zu\n",
		atomic_load_explicit(&allocs->timer_wait_allocs, memory_order_acquire),
		atomic_load_explicit(&allocs->timer_wait_frees, memory_order_acquire),
		atomic_load_explicit(&allocs->external_wait_allocs,
							 memory_order_acquire),
		atomic_load_explicit(&allocs->external_wait_frees,
							 memory_order_acquire));
	printf(
		"  total_requested_wait=%.2fms total_elapsed_wait=%.2fms total_cpu=%.2fms total_task_runtime=%.2fms\n",
		(double)total_requested / 1000.0, (double)total_elapsed / 1000.0,
		(double)total_cpu / 1000.0, (double)total_runtime / 1000.0);

	printf("\ntask metrics\n");
	printf(
		"  id runs timer external req_ms elapsed_ms avg_wait_ms max_wait_ms max_late_ms cpu_ms runtime_ms result\n");
	for (size_t index = 0; index < TOTAL_TASKS; index++) {
		const struct runtime_task_arg *task = &tasks[index];
		double avg_wait_ms = 0.0;

		if (task->wakes != 0)
			avg_wait_ms =
				(double)task->elapsed_wait_us / (double)task->wakes / 1000.0;

		printf(
			"  %2zu %4zu %5zu %8zu %7.2f %10.2f %11.2f %11.2f %11.2f %7.2f %10.2f %zu\n",
			task->id, atomic_load_explicit(&task->runs, memory_order_acquire),
			task->timer_waits, task->external_waits,
			(double)task->requested_wait_us / 1000.0,
			(double)task->elapsed_wait_us / 1000.0, avg_wait_ms,
			(double)task->max_wait_us / 1000.0,
			(double)task->max_late_us / 1000.0,
			(double)task->cpu_work_us / 1000.0,
			(double)task->runtime_us / 1000.0, task->result);
	}
	printf("\n");
}

int main(void) {
	struct allocation_stats allocs;
	struct timer_source_stats timer_stats;
	struct external_source_stats external_stats;
	struct external_source external;
	struct runtime_state runtime;
	struct runtime_task_arg tasks[TOTAL_TASKS];
	struct producer_arg producer;
	croutine_scheduler *scheduler = NULL;
	pthread_t external_thread;
	pthread_t producer_thread;
	croutine_config config = {
		.workers = RUNTIME_WORKERS,
		.main_queue_quota = 2,
		.main_event_source_config = {
			.factory_fn = timer_source_factory,
			.args = &timer_stats,
		},
	};
	uint32_t seed;
	size_t expected;
	size_t timer_waits;
	size_t external_waits;
	size_t stopped_iterations;
	size_t stopped_waits;
	size_t stopped_finished;
	size_t empty_start_wakes;
	int external_started = 0;
	int producer_started = 0;
	int scheduler_started = 0;
	int status = 0;

	clock_gettime(CLOCK_MONOTONIC, &log_start);
	seed = seed_from_env();
	atomic_init(&allocs.timer_wait_allocs, 0);
	atomic_init(&allocs.timer_wait_frees, 0);
	atomic_init(&allocs.external_wait_allocs, 0);
	atomic_init(&allocs.external_wait_frees, 0);
	atomic_init(&timer_stats.next_id, 0);
	atomic_init(&timer_stats.created, 0);
	atomic_init(&timer_stats.destroyed, 0);
	atomic_init(&timer_stats.registered, 0);
	atomic_init(&timer_stats.expired, 0);
	atomic_init(&timer_stats.local_wakes, 0);
	atomic_init(&timer_stats.blocking_waits, 0);
	atomic_init(&timer_stats.timed_waits, 0);
	atomic_init(&timer_stats.blocking_timeouts, 0);
	atomic_init(&timer_stats.idle_waits, 0);
	atomic_init(&timer_stats.wake_interrupts, 0);
	atomic_init(&timer_stats.suspends, 0);
	atomic_init(&timer_stats.collects, 0);
	atomic_init(&timer_stats.wakes, 0);
	atomic_init(&timer_stats.drained, 0);
	atomic_init(&timer_stats.failed, 0);
	atomic_init(&external_stats.registered, 0);
	atomic_init(&external_stats.early_rejected, 0);
	atomic_init(&external_stats.successful_wakes, 0);
	atomic_init(&external_stats.duplicate_rejected, 0);
	atomic_init(&external_stats.timed_waits, 0);
	atomic_init(&external_stats.drained, 0);
	atomic_init(&external_stats.failed, 0);
	atomic_init(&runtime.spawned, 0);
	atomic_init(&runtime.started, 0);
	atomic_init(&runtime.iterations, 0);
	atomic_init(&runtime.yields, 0);
	atomic_init(&runtime.waits_started, 0);
	atomic_init(&runtime.waits_completed, 0);
	atomic_init(&runtime.finished, 0);
	atomic_init(&runtime.failed, 0);

	if (external_source_init(&external, &external_stats) != 0) {
		fprintf(stderr, "failed to initialize external source\n");
		return 1;
	}
	if (pthread_create(&external_thread, NULL, external_source_main,
					   &external) != 0) {
		fprintf(stderr, "failed to start external source thread\n");
		external_source_destroy(&external);
		return 1;
	}
	external_started = 1;

	for (size_t index = 0; index < INITIAL_TASKS; index++)
		init_runtime_task(&tasks[index], &runtime, &allocs, &external, index,
						  4 + (index % 2), seed);
	for (size_t index = INITIAL_TASKS; index < TOTAL_TASKS; index++)
		init_runtime_task(&tasks[index], &runtime, &allocs, &external, index,
						  3 + ((index - INITIAL_TASKS) % 3), seed);
	expected = expected_iterations(tasks);

	log_event("runtime test create scheduler seed=0x%08x", seed);
	if (croutine_scheduler_create(&scheduler, &config) != 0) {
		fprintf(stderr, "failed to create scheduler\n");
		status = 1;
		goto cleanup;
	}

	if (atomic_load_explicit(&timer_stats.created, memory_order_acquire) !=
		RUNTIME_WORKERS) {
		fprintf(stderr, "main event source count mismatch\n");
		status = 1;
		goto cleanup;
	}

	log_event("main start scheduler with empty main queue");
	if (croutine_scheduler_start(scheduler) != 0) {
		fprintf(stderr, "failed to start scheduler\n");
		status = 1;
		goto cleanup;
	}
	scheduler_started = 1;

	log_event("main wait for empty startup suspend");
	if (wait_for_at_least(&timer_stats.idle_waits, RUNTIME_WORKERS, &runtime,
						  &timer_stats, &external_stats) != 0 ||
		wait_for_at_least(&timer_stats.suspends, RUNTIME_WORKERS, &runtime,
						  &timer_stats, &external_stats) != 0 ||
		wait_for_workers_suspended(scheduler, &runtime, &timer_stats,
								   &external_stats) != 0) {
		fprintf(stderr, "workers did not suspend after empty startup\n");
		status = 1;
		goto cleanup;
	}
	if (atomic_load_explicit(&runtime.spawned, memory_order_acquire) != 0 ||
		atomic_load_explicit(&runtime.started, memory_order_acquire) != 0 ||
		atomic_load_explicit(&runtime.finished, memory_order_acquire) != 0) {
		fprintf(stderr, "tasks ran before the initial injection\n");
		status = 1;
		goto cleanup;
	}

	empty_start_wakes =
		atomic_load_explicit(&timer_stats.wakes, memory_order_acquire);
	log_event("main inject initial tasks into empty scheduler");
	for (size_t index = 0; index < INITIAL_TASKS; index++) {
		log_event("main spawn initial task %zu", index);
		if (croutine_spawn(scheduler, runtime_task, &tasks[index]) != 0) {
			fprintf(stderr, "failed to spawn initial task %zu\n", index);
			status = 1;
			goto cleanup;
		}
		atomic_fetch_add_explicit(&runtime.spawned, 1, memory_order_acq_rel);
	}
	if (atomic_load_explicit(&timer_stats.wakes, memory_order_acquire) <=
		empty_start_wakes) {
		fprintf(stderr, "initial task injection did not wake event sources\n");
		status = 1;
		goto cleanup;
	}
	if (wait_for_at_least(&runtime.started, 1, &runtime, &timer_stats,
						  &external_stats) != 0) {
		fprintf(stderr, "workers did not run injected initial tasks\n");
		status = 1;
		goto cleanup;
	}

	log_event("main wait for %d first timer registrations", STOP_AFTER_WAITS);
	if (wait_for_at_least(&runtime.waits_started, STOP_AFTER_WAITS, &runtime,
						  &timer_stats, &external_stats) != 0) {
		fprintf(stderr, "runtime did not reach first wait checkpoint\n");
		status = 1;
		goto cleanup;
	}

	log_event("main manual stop scheduler");
	if (croutine_scheduler_stop(scheduler) != 0) {
		fprintf(stderr, "failed to stop scheduler\n");
		status = 1;
		goto cleanup;
	}
	scheduler_started = 0;

	stopped_iterations =
		atomic_load_explicit(&runtime.iterations, memory_order_acquire);
	stopped_waits =
		atomic_load_explicit(&runtime.waits_completed, memory_order_acquire);
	stopped_finished =
		atomic_load_explicit(&runtime.finished, memory_order_acquire);
	printf(
		"stop checkpoint: iterations=%zu waits_started=%zu waits_completed=%zu finished=%zu\n",
		stopped_iterations,
		atomic_load_explicit(&runtime.waits_started, memory_order_acquire),
		stopped_waits, stopped_finished);
	log_event("main pause while stopped %.2fs",
			  (double)STOP_PAUSE_US / 1000000.0);
	sleep_microseconds(STOP_PAUSE_US);
	if (atomic_load_explicit(&runtime.iterations, memory_order_acquire) !=
			stopped_iterations ||
		atomic_load_explicit(&runtime.waits_completed, memory_order_acquire) !=
			stopped_waits ||
		atomic_load_explicit(&runtime.finished, memory_order_acquire) !=
			stopped_finished) {
		fprintf(stderr, "tasks continued running after stop\n");
		status = 1;
		goto cleanup;
	}

	log_event("main resume scheduler");
	if (croutine_scheduler_start(scheduler) != 0) {
		fprintf(stderr, "failed to resume scheduler\n");
		status = 1;
		goto cleanup;
	}
	scheduler_started = 1;

	producer.scheduler = scheduler;
	producer.tasks = tasks;
	producer.first = INITIAL_TASKS;
	producer.count = PRODUCER_TASKS;
	producer.failed = &runtime.failed;
	log_event("main start producer thread");
	if (pthread_create(&producer_thread, NULL, producer_main, &producer) != 0) {
		fprintf(stderr, "failed to start producer thread\n");
		status = 1;
		goto cleanup;
	}
	producer_started = 1;

	if (pthread_join(producer_thread, NULL) != 0) {
		fprintf(stderr, "failed to join producer thread\n");
		status = 1;
		goto cleanup;
	}
	producer_started = 0;
	log_event("main producer joined");

	if (wait_for_at_least(&runtime.finished, TOTAL_TASKS, &runtime,
						  &timer_stats, &external_stats) != 0) {
		fprintf(stderr, "runtime tasks did not finish\n");
		status = 1;
		goto cleanup;
	}

	log_event("main wait for post-run event starvation");
	if (wait_for_workers_suspended(scheduler, &runtime, &timer_stats,
								   &external_stats) != 0) {
		fprintf(stderr, "workers did not suspend after all tasks finished\n");
		status = 1;
		goto cleanup;
	}

	log_event("main final stop scheduler from event-starved state");
	if (croutine_scheduler_stop(scheduler) != 0) {
		fprintf(stderr, "failed to stop scheduler at shutdown\n");
		status = 1;
		goto cleanup;
	}
	scheduler_started = 0;

cleanup:
	if (producer_started)
		(void)pthread_join(producer_thread, NULL);
	if (scheduler_started && scheduler != NULL)
		(void)croutine_scheduler_stop(scheduler);
	if (external_started) {
		external_source_stop(&external);
		(void)pthread_join(external_thread, NULL);
		external_started = 0;
	}
	if (scheduler != NULL) {
		log_event("main destroy scheduler");
		if (croutine_scheduler_destroy(scheduler) != 0) {
			fprintf(stderr, "failed to destroy scheduler\n");
			status = 1;
		}
	}
	external_source_destroy(&external);

	sum_task_waits(tasks, &timer_waits, &external_waits);
	if (atomic_load_explicit(&runtime.failed, memory_order_acquire) != 0 ||
		atomic_load_explicit(&timer_stats.failed, memory_order_acquire) != 0 ||
		atomic_load_explicit(&external_stats.failed, memory_order_acquire) != 0)
		status = 1;
	if (verify_tasks(tasks, atomic_load_explicit(&runtime.iterations,
												 memory_order_acquire)) != 0)
		status = 1;
	if (verify_stopped_deadline_resume(tasks) != 0)
		status = 1;
	if (atomic_load_explicit(&runtime.spawned, memory_order_acquire) !=
			TOTAL_TASKS ||
		atomic_load_explicit(&runtime.started, memory_order_acquire) !=
			TOTAL_TASKS ||
		atomic_load_explicit(&runtime.finished, memory_order_acquire) !=
			TOTAL_TASKS) {
		fprintf(stderr, "task lifecycle count mismatch\n");
		status = 1;
	}
	if (atomic_load_explicit(&runtime.waits_started, memory_order_acquire) !=
			expected ||
		atomic_load_explicit(&runtime.waits_completed, memory_order_acquire) !=
			expected) {
		fprintf(stderr, "wait lifecycle count mismatch\n");
		status = 1;
	}
	if (atomic_load_explicit(&timer_stats.registered, memory_order_acquire) !=
			timer_waits ||
		atomic_load_explicit(&timer_stats.expired, memory_order_acquire) !=
			timer_waits ||
		atomic_load_explicit(&timer_stats.local_wakes, memory_order_acquire) !=
			timer_waits) {
		fprintf(stderr, "timer source count mismatch\n");
		status = 1;
	}
	if (atomic_load_explicit(&external_stats.registered,
							 memory_order_acquire) != external_waits ||
		atomic_load_explicit(&external_stats.early_rejected,
							 memory_order_acquire) != external_waits ||
		atomic_load_explicit(&external_stats.successful_wakes,
							 memory_order_acquire) != external_waits ||
		atomic_load_explicit(&external_stats.duplicate_rejected,
							 memory_order_acquire) != external_waits) {
		fprintf(stderr, "external source count mismatch\n");
		status = 1;
	}
	if (timer_waits == 0 || external_waits == 0) {
		fprintf(stderr, "test did not cover both timer and external waits\n");
		status = 1;
	}
	if (atomic_load_explicit(&timer_stats.created, memory_order_acquire) !=
			RUNTIME_WORKERS ||
		atomic_load_explicit(&timer_stats.destroyed, memory_order_acquire) !=
			RUNTIME_WORKERS) {
		fprintf(stderr, "main event source destroy count mismatch\n");
		status = 1;
	}
	if (atomic_load_explicit(&timer_stats.blocking_waits,
							 memory_order_acquire) == 0 ||
		atomic_load_explicit(&timer_stats.timed_waits, memory_order_acquire) ==
			0 ||
		atomic_load_explicit(&timer_stats.blocking_timeouts,
							 memory_order_acquire) == 0 ||
		atomic_load_explicit(&timer_stats.idle_waits, memory_order_acquire) <
			RUNTIME_WORKERS * 2 ||
		atomic_load_explicit(&timer_stats.collects, memory_order_acquire) ==
			0 ||
		atomic_load_explicit(&timer_stats.wake_interrupts,
							 memory_order_acquire) == 0 ||
		atomic_load_explicit(&timer_stats.suspends, memory_order_acquire) <
			RUNTIME_WORKERS * 2) {
		fprintf(stderr,
				"timer blocking/collect paths were not all exercised\n");
		status = 1;
	}
	if (atomic_load_explicit(&external_stats.timed_waits,
							 memory_order_acquire) == 0) {
		fprintf(stderr, "external timed wait path was not exercised\n");
		status = 1;
	}
	if (verify_allocations(&allocs) != 0)
		status = 1;

	print_runtime_report(tasks, &runtime, &timer_stats, &external_stats,
						 &allocs, expected);
	return status;
}
