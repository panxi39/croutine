#ifndef CROUTINE_INTERNAL_STACK_H
#define CROUTINE_INTERNAL_STACK_H

#include "types.h"

#define CROUTINE_STACK_ERROR ((struct croutine_stack*)-1)

struct croutine_stack *croutine_stack_alloc(size_t size);
void croutine_stack_free(struct croutine_stack *stack);

#endif
