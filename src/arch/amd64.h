#ifndef CROUTINE_ARCH_AMD64_H
#define CROUTINE_ARCH_AMD64_H

#include <stdint.h>

struct croutine_arch_context {
	void *rsp;
	uintptr_t rbx;
	uintptr_t rbp;
	uintptr_t r12;
	uintptr_t r13;
	uintptr_t r14;
	uintptr_t r15;
};

#endif
