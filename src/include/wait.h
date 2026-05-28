#ifndef CROUTINE_INTERNAL_WAIT_H
#define CROUTINE_INTERNAL_WAIT_H

#include <stdint.h>

#include "types.h"

int croutine_wait_handle_init_simple(struct croutine_wait_handle *handle,
									 struct croutine_task *task, void *data,
									 int (*checker)(void *data));
int croutine_wait_handle_init_complex(struct croutine_wait_handle *handle,
									  struct croutine_task *task, uint32_t refs,
									  void *data, int (*checker)(void *data));

#endif
