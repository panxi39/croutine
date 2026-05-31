#include "croutine_event.h"
#include "task.h"
#include "wait.h"

#include <stdatomic.h>
#include <string.h>

static void croutine_wait_handle_init(struct croutine_wait_handle *handle,
									  struct croutine_task *task,
									  enum croutine_wait_handle_type type,
									  void *data, int (*checker)(void *data)) {
	memset(handle, 0, sizeof(*handle));
	croutine_refcount_init(&handle->refcount);
	handle->scheduler = task != NULL ? task->scheduler : NULL;
	handle->task = task;
	handle->type = type;
	atomic_init(&handle->state, CROUTINE_WAIT_HANDLE_PENDING);
	handle->data = data;
	handle->checker = checker;
}

int croutine_wait_handle_init_default(struct croutine_wait_handle *handle,
									  struct croutine_task *task, void *data,
									  int (*checker)(void *data)) {
	if (handle == NULL || task == NULL)
		return -1;

	croutine_wait_handle_init(handle, task, CROUTINE_WAIT_HANDLE_SIMPLE, data,
							  checker);
	return 0;
}

int croutine_wait_handle_init_complex(struct croutine_wait_handle *handle,
									  struct croutine_task *task, uint32_t refs,
									  void *data, int (*checker)(void *data)) {
	if (handle == NULL || task == NULL || refs == 0)
		return -1;

	croutine_wait_handle_init(handle, task, CROUTINE_WAIT_HANDLE_COMPLEX, data,
							  checker);
	atomic_store_explicit(&handle->refcount.refs, refs, memory_order_release);
	return 0;
}

int croutine_wait_handle_wake(croutine_wait_handle *handle) {
	enum croutine_wait_handle_state expected;

	if (handle == NULL || handle->task == NULL)
		return -1;

	expected = CROUTINE_WAIT_HANDLE_PENDING;
	if (!atomic_compare_exchange_strong_explicit(
			&handle->state, &expected, CROUTINE_WAIT_HANDLE_PROCESSING,
			memory_order_acq_rel, memory_order_acquire))
		return -1;

	if (handle->checker != NULL && handle->checker(handle->data) != 1) {
		atomic_store_explicit(&handle->state, CROUTINE_WAIT_HANDLE_PENDING,
							  memory_order_release);
		return -1;
	}

	atomic_store_explicit(&handle->state, CROUTINE_WAIT_HANDLE_FINISHED,
						  memory_order_release);
	if (croutine_task_wake(handle->task) != 0) {
		atomic_store_explicit(&handle->state, CROUTINE_WAIT_HANDLE_PENDING,
							  memory_order_release);
		return -1;
	}

	return 0;
}

int croutine_wait_handle_get(croutine_wait_handle *handle) {
	if (handle == NULL || handle->type != CROUTINE_WAIT_HANDLE_COMPLEX)
		return -1;

	return croutine_refcount_get(&handle->refcount);
}

int croutine_wait_handle_release(croutine_wait_handle *handle) {
	if (handle == NULL || handle->type != CROUTINE_WAIT_HANDLE_COMPLEX)
		return -1;

	return croutine_refcount_release(&handle->refcount, NULL);
}
