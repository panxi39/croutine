#if defined(__aarch64__)

#include "arch.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static_assert(offsetof(struct croutine_arch_context, sp) == 0,
			  "aarch64 sp offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x19) == 8,
			  "aarch64 x19 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x21) == 24,
			  "aarch64 x21 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x23) == 40,
			  "aarch64 x23 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x25) == 56,
			  "aarch64 x25 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x27) == 72,
			  "aarch64 x27 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, x29) == 88,
			  "aarch64 x29 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, d8) == 104,
			  "aarch64 d8 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, d10) == 120,
			  "aarch64 d10 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, d12) == 136,
			  "aarch64 d12 offset mismatch");
static_assert(offsetof(struct croutine_arch_context, d14) == 152,
			  "aarch64 d14 offset mismatch");

int croutine_arch_context_init(struct croutine_arch_context *context,
							   void *stack_base, size_t stack_size,
							   croutine_arch_entry entry) {
	uintptr_t low;
	uintptr_t sp;

	if (context == NULL || stack_base == NULL || stack_size < 64 ||
		entry == NULL)
		return -1;

	memset(context, 0, sizeof(*context));

	low = (uintptr_t)stack_base;
	sp = low + stack_size;
	sp &= ~(uintptr_t)0xf;
	if (sp <= low)
		return -1;

	context->sp = (void *)sp;
	context->x30 = (uintptr_t)entry;
	return 0;
}

#endif
