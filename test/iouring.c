#define _POSIX_C_SOURCE 200809L

#include "croutine.h"
#include "croutine_event.h"
#include "task.h"
#include "types.h"
#include "wait.h"

#include <arpa/inet.h>
#include <errno.h>
#include <liburing.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define IOURING_QUEUE_DEPTH 64
#define IOURING_UDP_PACKETS 1024
#define IOURING_WAIT_ROUNDS 5000
#define IOURING_WAIT_US 1000L
#define IOURING_SEND_GAP_US 1000L

struct iouring_stats {
	_Atomic size_t sources_created;
	_Atomic size_t submitted;
	_Atomic size_t completed;
	_Atomic size_t recv_ops;
	_Atomic int failed;
};

struct iouring_source {
	croutine_main_event_source base;
	croutine_worker *worker;
	struct io_uring ring;
	int cqe_eventfd;
	int wake_eventfd;
	int resume_eventfd;
	size_t inflight;
	struct iouring_stats *stats;
};

struct iouring_op {
	croutine_wait_handle handle;
	struct iouring_source *source;
	int res;
};

struct receiver_arg {
	int socket_fd;
	_Atomic size_t received;
	_Atomic int done;
	_Atomic int failed;
};

struct sender_arg {
	uint16_t port;
	_Atomic int stopping;
	_Atomic size_t sent;
	_Atomic int failed;
};

static void sleep_microseconds(long microseconds) {
	struct timespec delay;

	delay.tv_sec = microseconds / 1000000L;
	delay.tv_nsec = (microseconds % 1000000L) * 1000L;
	while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
	}
}

static uint64_t random_step(uint64_t *state) {
	uint64_t value = *state;

	value ^= value << 13;
	value ^= value >> 7;
	value ^= value << 17;
	*state = value;
	return value;
}

static void iouring_drain_eventfd(int fd) {
	eventfd_t value;

	if (fd < 0)
		return;
	while (eventfd_read(fd, &value) == 0) {
	}
}

static int iouring_signal_eventfd(int fd) {
	int ret;

	do {
		ret = eventfd_write(fd, 1);
	} while (ret != 0 && errno == EINTR);
	if (ret == 0 || errno == EAGAIN)
		return 0;
	return -1;
}

static struct iouring_source *iouring_source_from_base(croutine_main_event_source *source) {
	return (struct iouring_source *)source;
}

static struct iouring_source *iouring_current_source(void) {
	struct croutine_task *task = croutine_task_current();
	struct croutine_worker *worker;

	if (task == NULL)
		return NULL;
	worker = atomic_load_explicit(&task->worker, memory_order_relaxed);
	if (worker == NULL || worker->main_event_source == NULL)
		return NULL;
	return iouring_source_from_base(worker->main_event_source);
}

static void iouring_collect(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);
	struct io_uring_cqe *cqe;

	if (source == NULL)
		return;

	iouring_drain_eventfd(source->wake_eventfd);
	iouring_drain_eventfd(source->cqe_eventfd);
	while (io_uring_peek_cqe(&source->ring, &cqe) == 0) {
		struct iouring_op *op = io_uring_cqe_get_data(cqe);

		if (op == NULL) {
			atomic_store_explicit(&source->stats->failed, 1, memory_order_release);
			io_uring_cqe_seen(&source->ring, cqe);
			continue;
		}

		op->res = cqe->res;
		if (source->inflight == 0)
			atomic_store_explicit(&source->stats->failed, 1, memory_order_release);
		else
			source->inflight--;
		atomic_fetch_add_explicit(&source->stats->completed, 1, memory_order_acq_rel);
		atomic_fetch_add_explicit(&source->stats->recv_ops, 1, memory_order_acq_rel);
		if (croutine_wait_handle_wake(&op->handle) != 0)
			atomic_store_explicit(&source->stats->failed, 1, memory_order_release);
		io_uring_cqe_seen(&source->ring, cqe);
	}
}

static enum croutine_main_event_wait_result iouring_blocking_wait(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);
	struct pollfd fds[2];
	int ret;

	if (source == NULL)
		return CROUTINE_MAIN_EVENT_WAIT_ERROR;

	fds[0].fd = source->cqe_eventfd;
	fds[0].events = POLLIN;
	fds[0].revents = 0;
	fds[1].fd = source->wake_eventfd;
	fds[1].events = POLLIN;
	fds[1].revents = 0;

	do {
		ret = poll(fds, 2, -1);
	} while (ret < 0 && errno == EINTR);
	if (ret < 0)
		return CROUTINE_MAIN_EVENT_WAIT_ERROR;

	return CROUTINE_MAIN_EVENT_WAIT_DONE;
}

static int iouring_wake(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);

	if (source == NULL)
		return -1;
	return iouring_signal_eventfd(source->wake_eventfd);
}

static void iouring_suspend(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);
	struct pollfd fd;
	int ret;

	if (source == NULL)
		return;

	fd.fd = source->resume_eventfd;
	fd.events = POLLIN;
	fd.revents = 0;
	do {
		ret = poll(&fd, 1, -1);
	} while (ret < 0 && errno == EINTR);
	if (ret > 0 && (fd.revents & POLLIN) != 0)
		iouring_drain_eventfd(source->resume_eventfd);
}

static void iouring_resume(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);

	if (source == NULL || iouring_signal_eventfd(source->resume_eventfd) != 0)
		abort();
}

static void iouring_destroy(croutine_main_event_source *base) {
	struct iouring_source *source = iouring_source_from_base(base);

	if (source == NULL)
		return;
	if (source->inflight != 0)
		atomic_store_explicit(&source->stats->failed, 1, memory_order_release);
	(void)io_uring_unregister_eventfd(&source->ring);
	io_uring_queue_exit(&source->ring);
	if (source->cqe_eventfd >= 0)
		close(source->cqe_eventfd);
	if (source->wake_eventfd >= 0)
		close(source->wake_eventfd);
	if (source->resume_eventfd >= 0)
		close(source->resume_eventfd);
	free(source);
}

static croutine_main_event_source *iouring_source_factory(croutine_worker *worker, void *args) {
	struct iouring_stats *stats = args;
	struct iouring_source *source;
	int ret;

	if (worker == NULL || stats == NULL)
		return NULL;

	source = calloc(1, sizeof(*source));
	if (source == NULL)
		return NULL;
	source->worker = worker;
	source->stats = stats;
	source->cqe_eventfd = -1;
	source->wake_eventfd = -1;
	source->resume_eventfd = -1;

	source->cqe_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (source->cqe_eventfd < 0)
		goto fail;
	source->wake_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (source->wake_eventfd < 0)
		goto fail;
	source->resume_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (source->resume_eventfd < 0)
		goto fail;

	ret = io_uring_queue_init(IOURING_QUEUE_DEPTH, &source->ring, 0);
	if (ret < 0)
		goto fail;
	ret = io_uring_register_eventfd(&source->ring, source->cqe_eventfd);
	if (ret < 0) {
		io_uring_queue_exit(&source->ring);
		goto fail;
	}

	source->base.blocking_wait = iouring_blocking_wait;
	source->base.collect = iouring_collect;
	source->base.wake = iouring_wake;
	source->base.suspend = iouring_suspend;
	source->base.resume = iouring_resume;
	source->base.destroy = iouring_destroy;
	atomic_fetch_add_explicit(&stats->sources_created, 1, memory_order_acq_rel);
	return &source->base;

fail:
	if (source->cqe_eventfd >= 0)
		close(source->cqe_eventfd);
	if (source->wake_eventfd >= 0)
		close(source->wake_eventfd);
	if (source->resume_eventfd >= 0)
		close(source->resume_eventfd);
	free(source);
	return NULL;
}

static int iouring_submit_current(struct iouring_source *source) {
	int ret;

	source->inflight++;
	atomic_fetch_add_explicit(&source->stats->submitted, 1, memory_order_acq_rel);
	ret = io_uring_submit(&source->ring);
	if (ret < 1) {
		source->inflight--;
		atomic_store_explicit(&source->stats->failed, 1, memory_order_release);
		return -1;
	}
	return 0;
}

static ssize_t iouring_async_recv(int fd, void *buffer, size_t length) {
	struct iouring_source *source;
	struct iouring_op *op;
	struct io_uring_sqe *sqe;
	struct croutine_task *task;
	int res;

	source = iouring_current_source();
	task = croutine_task_current();
	if (source == NULL || task == NULL || buffer == NULL || length == 0)
		return -1;

	op = calloc(1, sizeof(*op));
	if (op == NULL)
		return -1;
	op->source = source;
	if (croutine_wait_handle_init_default(&op->handle, task, op, NULL) != 0)
		goto fail_free;

	if (croutine_await() != 0)
		goto fail_free;

	sqe = io_uring_get_sqe(&source->ring);
	if (sqe == NULL)
		goto fail_cancel;
	io_uring_prep_recv(sqe, fd, buffer, length, 0);
	io_uring_sqe_set_data(sqe, op);
	if (iouring_submit_current(source) != 0)
		abort();

	croutine_yield();
	res = op->res;
	free(op);
	return res;

fail_cancel:
	if (croutine_cancel_await() != 0)
		abort();
fail_free:
	free(op);
	return -1;
}

static int create_udp_socket(uint16_t *port) {
	struct sockaddr_in addr;
	socklen_t addrlen;
	int fd;
	int yes = 1;

	fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
	if (fd < 0)
		return -1;
	(void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
		goto fail;

	addrlen = sizeof(addr);
	if (getsockname(fd, (struct sockaddr *)&addr, &addrlen) != 0)
		goto fail;
	*port = ntohs(addr.sin_port);
	return fd;

fail:
	close(fd);
	return -1;
}

static void *receiver_task(void *arg) {
	struct receiver_arg *receiver = arg;

	for (size_t index = 0; index < IOURING_UDP_PACKETS; index++) {
		uint64_t value;
		ssize_t nread;

		nread = iouring_async_recv(receiver->socket_fd, &value, sizeof(value));
		if (nread != (ssize_t)sizeof(value)) {
			fprintf(stderr, "iouring udp recv failed at %zu: %zd\n", index, nread);
			atomic_store_explicit(&receiver->failed, 1, memory_order_release);
			return NULL;
		}

		atomic_fetch_add_explicit(&receiver->received, 1, memory_order_acq_rel);
		printf("iouring udp recv %02zu 0x%016llx\n", index, (unsigned long long)value);
	}

	atomic_store_explicit(&receiver->done, 1, memory_order_release);
	return NULL;
}

static void *sender_main(void *arg) {
	struct sender_arg *sender = arg;
	struct sockaddr_in addr;
	uint64_t random = 0x6eed0e9da4d94a4full;
	int fd;

	fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
	if (fd < 0)
		goto fail;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons(sender->port);

	while (atomic_load_explicit(&sender->stopping, memory_order_acquire) == 0) {
		uint64_t value = random_step(&random);
		ssize_t written;

		written = sendto(fd, &value, sizeof(value), 0, (struct sockaddr *)&addr, sizeof(addr));
		if (written != (ssize_t)sizeof(value)) {
			if (written < 0 && errno == EINTR)
				continue;
			close(fd);
			goto fail;
		}
		atomic_fetch_add_explicit(&sender->sent, 1, memory_order_acq_rel);
		sleep_microseconds(IOURING_SEND_GAP_US);
	}

	close(fd);
	return NULL;

fail:
	atomic_store_explicit(&sender->failed, 1, memory_order_release);
	return NULL;
}

static int iouring_available(void) {
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(2, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "io_uring unavailable, skipping test: %s\n", strerror(-ret));
		return 0;
	}
	io_uring_queue_exit(&ring);
	return 1;
}

int main(void) {
	struct iouring_stats stats;
	struct receiver_arg receiver;
	struct sender_arg sender;
	croutine_scheduler *scheduler = NULL;
	pthread_t sender_thread;
	croutine_config config;
	uint16_t port = 0;
	int socket_fd = -1;
	int sender_started = 0;
	int scheduler_started = 0;
	int status = 0;

	if (!iouring_available())
		return 0;

	socket_fd = create_udp_socket(&port);
	if (socket_fd < 0) {
		fprintf(stderr, "failed to create UDP socket\n");
		return 1;
	}

	memset(&stats, 0, sizeof(stats));
	atomic_init(&stats.sources_created, 0);
	atomic_init(&stats.submitted, 0);
	atomic_init(&stats.completed, 0);
	atomic_init(&stats.recv_ops, 0);
	atomic_init(&stats.failed, 0);

	memset(&receiver, 0, sizeof(receiver));
	receiver.socket_fd = socket_fd;
	atomic_init(&receiver.received, 0);
	atomic_init(&receiver.done, 0);
	atomic_init(&receiver.failed, 0);

	memset(&sender, 0, sizeof(sender));
	sender.port = port;
	atomic_init(&sender.stopping, 0);
	atomic_init(&sender.sent, 0);
	atomic_init(&sender.failed, 0);

	memset(&config, 0, sizeof(config));
	config.workers = 1;
	config.queue_check_interval = 1;
	config.main_event_source_config.factory_fn = iouring_source_factory;
	config.main_event_source_config.args = &stats;

	if (croutine_scheduler_create(&scheduler, &config) != 0) {
		fprintf(stderr, "failed to create scheduler\n");
		status = 1;
		goto cleanup;
	}
	if (croutine_scheduler_start(scheduler) != 0) {
		fprintf(stderr, "failed to start scheduler\n");
		status = 1;
		goto cleanup;
	}
	scheduler_started = 1;

	if (croutine_spawn(scheduler, receiver_task, &receiver) != 0) {
		fprintf(stderr, "failed to spawn iouring receiver task\n");
		status = 1;
		goto cleanup;
	}
	if (pthread_create(&sender_thread, NULL, sender_main, &sender) != 0) {
		fprintf(stderr, "failed to start UDP sender thread\n");
		status = 1;
		goto cleanup;
	}
	sender_started = 1;

	for (size_t round = 0; round < IOURING_WAIT_ROUNDS; round++) {
		if (atomic_load_explicit(&receiver.done, memory_order_acquire) != 0 ||
			atomic_load_explicit(&receiver.failed, memory_order_acquire) != 0 ||
			atomic_load_explicit(&sender.failed, memory_order_acquire) != 0)
			break;
		sleep_microseconds(IOURING_WAIT_US);
	}

	atomic_store_explicit(&sender.stopping, 1, memory_order_release);
	if (sender_started) {
		(void)pthread_join(sender_thread, NULL);
		sender_started = 0;
	}

	if (atomic_load_explicit(&receiver.done, memory_order_acquire) == 0 ||
		atomic_load_explicit(&receiver.failed, memory_order_acquire) != 0 ||
		atomic_load_explicit(&sender.failed, memory_order_acquire) != 0 ||
		atomic_load_explicit(&stats.failed, memory_order_acquire) != 0) {
		fprintf(stderr, "iouring UDP test failed\n");
		status = 1;
	}
	if (atomic_load_explicit(&receiver.received, memory_order_acquire) != IOURING_UDP_PACKETS ||
		atomic_load_explicit(&stats.submitted, memory_order_acquire) != IOURING_UDP_PACKETS ||
		atomic_load_explicit(&stats.completed, memory_order_acquire) != IOURING_UDP_PACKETS ||
		atomic_load_explicit(&stats.recv_ops, memory_order_acquire) != IOURING_UDP_PACKETS) {
		fprintf(stderr, "iouring UDP stats mismatch\n");
		status = 1;
	}

cleanup:
	atomic_store_explicit(&sender.stopping, 1, memory_order_release);
	if (sender_started)
		(void)pthread_join(sender_thread, NULL);
	if (scheduler_started && scheduler != NULL) {
		(void)croutine_scheduler_stop(scheduler);
		scheduler_started = 0;
	}
	if (scheduler != NULL)
		(void)croutine_scheduler_destroy(scheduler);
	if (socket_fd >= 0)
		close(socket_fd);

	return status;
}
