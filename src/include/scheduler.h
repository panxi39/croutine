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

#endif
