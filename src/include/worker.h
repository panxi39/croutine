#ifndef CROUTINE_INTERNAL_WORKER_H
#define CROUTINE_INTERNAL_WORKER_H

#include "types.h"

int croutine_worker_init(struct croutine_worker *worker,
						 struct croutine_scheduler *scheduler);
int croutine_worker_start(struct croutine_worker *worker);
int croutine_worker_join(struct croutine_worker *worker);
void croutine_worker_destroy(struct croutine_worker *worker);
void croutine_worker_schedule(void);
int croutine_worker_enqueue_local(struct croutine_worker *worker,
								  struct croutine_task *task);
void croutine_worker_request_suspend(struct croutine_worker *worker);
void croutine_worker_wake(struct croutine_worker *worker);
void croutine_worker_request_exit(struct croutine_worker *worker);

#endif
