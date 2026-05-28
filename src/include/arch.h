#ifndef CROUTINE_ARCH_H
#define CROUTINE_ARCH_H

#include <stddef.h>

#if defined(__x86_64__) || defined(__amd64__)
#include "../arch/amd64.h"
#elif defined(__aarch64__)
#include "../arch/aarch64.h"
#else
#error "unsupported croutine architecture"
#endif

typedef struct croutine_arch_context croutine_arch_context;

typedef void (*croutine_arch_entry)(void);
typedef void (*croutine_arch_call)(void);

int croutine_arch_context_init(struct croutine_arch_context *context,
							   void *stack_base, size_t stack_size,
							   croutine_arch_entry entry);

void croutine_arch_store_and_call(struct croutine_arch_context *context,
								  croutine_arch_call func);
void croutine_arch_resume_and_ret(struct croutine_arch_context *context);

#endif
