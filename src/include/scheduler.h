#ifndef CROUTINE_INTERNAL_SCHEDULER_H
#define CROUTINE_INTERNAL_SCHEDULER_H

#include "types.h"

int croutine_scheduler_enqueue_main(struct croutine_scheduler *scheduler,
									struct croutine_task *task);
struct croutine_task *
croutine_scheduler_pop_main(struct croutine_worker *worker);
void croutine_scheduler_add_finished(struct croutine_scheduler *scheduler,
									 struct croutine_task *task);
void croutine_scheduler_reclaim_task(struct croutine_scheduler *scheduler,
									 struct croutine_task *task);

int croutine_scheduler_is_stopping(struct croutine_scheduler *scheduler);
int croutine_scheduler_is_destroying(struct croutine_scheduler *scheduler);
void croutine_scheduler_worker_quiescent(struct croutine_worker *worker);
void croutine_scheduler_wake_all(struct croutine_scheduler *scheduler);
void croutine_scheduler_resume_all(struct croutine_scheduler *scheduler);

#endif
