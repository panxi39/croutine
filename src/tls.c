#include "tls.h"

_Thread_local struct croutine_scheduler *croutine_sched CROUTINE_TLS_HIDDEN;
_Thread_local struct croutine_worker *croutine_current_worker
	CROUTINE_TLS_HIDDEN;
_Thread_local struct croutine_task *croutine_current_task CROUTINE_TLS_HIDDEN;
