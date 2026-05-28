#if defined(__x86_64__) || defined(__amd64__)

#include "arch.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static_assert(offsetof(struct croutine_arch_context, rsp) == 0,
			  "amd64 rsp offset mismatch");
static_assert(offsetof(struct croutine_arch_context, rbx) == 8,
			  "amd64 rbx offset mismatch");
static_assert(offsetof(struct croutine_arch_context, rbp) == 16,
			  "amd64 rbp offset mismatch");
static_assert(offsetof(struct croutine_arch_context, r12) == 24,
			  "amd64 r12 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, r13) == 32,
			  "amd64 r13 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, r14) == 40,
			  "amd64 r14 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, r15) == 48,
			  "amd64 r15 offset mismatch");

int croutine_arch_context_init(struct croutine_arch_context *context,
							   void *stack_base, size_t stack_size,
							   croutine_arch_entry entry) {
	uintptr_t low;
	uintptr_t sp;
	uintptr_t *stack;

	if (context == NULL || stack_base == NULL || stack_size < 64 ||
		entry == NULL)
		return -1;

	memset(context, 0, sizeof(*context));

	low = (uintptr_t)stack_base;
	sp = low + stack_size;
	sp &= ~(uintptr_t)0xf;
	if (sp <= low + 16)
		return -1;

	stack = (uintptr_t *)sp - 2;
	stack[0] = (uintptr_t)entry;
	stack[1] = 0;

	context->rsp = stack;
	return 0;
}

#endif
