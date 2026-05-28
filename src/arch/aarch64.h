#ifndef CROUTINE_ARCH_AARCH64_H
#define CROUTINE_ARCH_AARCH64_H

#include <stdint.h>

struct croutine_arch_context {
	void *sp;
	uintptr_t x19;
	uintptr_t x20;
	uintptr_t x21;
	uintptr_t x22;
	uintptr_t x23;
	uintptr_t x24;
	uintptr_t x25;
	uintptr_t x26;
	uintptr_t x27;
	uintptr_t x28;
	uintptr_t x29;
	uintptr_t x30;
	uint64_t d8;
	uint64_t d9;
	uint64_t d10;
	uint64_t d11;
	uint64_t d12;
	uint64_t d13;
	uint64_t d14;
	uint64_t d15;
};

#endif
