#define _GNU_SOURCE

#include "stack.h"

#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>


static int croutine_stack_align_size(size_t size, size_t page_size,
									 size_t *aligned) {
	size_t mask;

	if (size == 0 || page_size == 0 || aligned == NULL)
		return -1;

	mask = page_size - 1;
	if ((page_size & mask) != 0 || size > SIZE_MAX - mask)
		return -1;

	*aligned = (size + mask) & ~mask;
	return 0;
}

struct croutine_stack *croutine_stack_alloc(size_t size) {
	struct croutine_stack *stack;
	void *mapping;
	size_t aligned_size;
	size_t mapping_size;
	size_t page_size;
	long sys_page_size;

	sys_page_size = sysconf(_SC_PAGESIZE);
	if (sys_page_size <= 0)
		return CROUTINE_STACK_ERROR;
	page_size = (size_t)sys_page_size;

	if (croutine_stack_align_size(size, page_size, &aligned_size) != 0 ||
		aligned_size > SIZE_MAX - page_size)
		return CROUTINE_STACK_ERROR;
	mapping_size = page_size + aligned_size;

	stack = malloc(sizeof(*stack));
	if (stack == NULL)
		return CROUTINE_STACK_ERROR;

	mapping = mmap(NULL, mapping_size, PROT_READ | PROT_WRITE,
				   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (mapping == MAP_FAILED) {
		free(stack);
		return CROUTINE_STACK_ERROR;
	}

	if (mprotect(mapping, page_size, PROT_NONE) != 0) {
		munmap(mapping, mapping_size);
		free(stack);
		return CROUTINE_STACK_ERROR;
	}

	stack->mmap_base = mapping;
	stack->mmap_size = mapping_size;
	stack->bottom = (char *)mapping + page_size;
	stack->top = (char *)stack->bottom + size;
	stack->size = size;
	return stack;
}

void croutine_stack_free(struct croutine_stack *stack) {
	if (stack == NULL || stack == CROUTINE_STACK_ERROR)
		return;

	(void)munmap(stack->mmap_base, stack->mmap_size);
	free(stack);
}
