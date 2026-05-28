#ifndef CROUTINE_INTERNAL_TLS_H
#define CROUTINE_INTERNAL_TLS_H

#if defined(__GNUC__) || defined(__clang__)
#define CROUTINE_TLS_HIDDEN __attribute__((visibility("hidden")))
#else
#define CROUTINE_TLS_HIDDEN
#endif

struct croutine_scheduler;
struct croutine_task;
struct croutine_worker;

extern _Thread_local struct croutine_scheduler *croutine_sched
	CROUTINE_TLS_HIDDEN;
extern _Thread_local struct croutine_worker *croutine_current_worker
	CROUTINE_TLS_HIDDEN;
extern _Thread_local struct croutine_task *croutine_current_task
	CROUTINE_TLS_HIDDEN;

#endif
