#ifndef CROUTINE_INTERNAL_TASK_H
#define CROUTINE_INTERNAL_TASK_H

#include <stddef.h>

#include "types.h"

int croutine_task_init(struct croutine_task *task,
					   struct croutine_scheduler *scheduler,
					   croutine_task_fn func, void *arg);
void croutine_task_init_current(struct croutine_task *task);
void croutine_task_enter_scheduler(void);
void croutine_task_resume(struct croutine_task *task);
int croutine_task_prepare_wait(void);
int croutine_task_cancel_wait(void);
void croutine_task_wait(void);
enum croutine_task_enqueue_result
croutine_task_enqueue(struct croutine_task *task);
int croutine_task_wake(struct croutine_task *task);

#endif
