#ifndef CROUTINE_INTERNAL_SCHEDULER_H
#define CROUTINE_INTERNAL_SCHEDULER_H

#include "types.h"

int croutine_scheduler_enqueue_main(struct croutine_scheduler *scheduler,
									struct croutine_task *task);
void croutine_scheduler_wake_one(
	struct croutine_scheduler *scheduler);
void croutine_scheduler_add_finished(struct croutine_scheduler *scheduler,
									 struct croutine_task *task);
void croutine_scheduler_reclaim_task(struct croutine_scheduler *scheduler,
									 struct croutine_task *task);

#endif
