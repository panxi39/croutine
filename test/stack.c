#include "stack.h"

#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>

#define TEST_STACK_SIZE ((size_t)65537)

static int check(int condition, const char *message) {
	if (!condition) {
		fprintf(stderr, "%s\n", message);
		return -1;
	}

	return 0;
}

static int test_invalid_size(void) {
	return check(croutine_stack_alloc(0) == (struct croutine_stack *)-1,
				 "zero-sized stack allocation should fail");
}

static int test_stack_layout(void) {
	struct croutine_stack *stack;
	volatile unsigned char *bottom;
	volatile unsigned char *top;
	int status = 0;

	stack = croutine_stack_alloc(TEST_STACK_SIZE);
	if (check(stack != (struct croutine_stack *)-1,
			  "stack allocation should succeed") != 0)
		return -1;

	if (check(stack->size == TEST_STACK_SIZE,
			  "stack should preserve requested usable size") != 0)
		status = -1;
	if (check((char *)stack->bottom < (char *)stack->top,
			  "stack bottom should precede top") != 0)
		status = -1;
	if (check((char *)stack->top == (char *)stack->bottom + TEST_STACK_SIZE,
			  "stack top should match requested usable size") != 0)
		status = -1;
	if (check((char *)stack->mmap_base < (char *)stack->bottom,
			  "mapping should include guard page before bottom") != 0)
		status = -1;

	bottom = stack->bottom;
	top = (unsigned char *)stack->top - 1;
	*bottom = 0xa5;
	*top = 0x5a;

	croutine_stack_free(stack);
	return status;
}

static int test_guard_page(void) {
	struct croutine_stack *stack;
	pid_t pid;
	int status;

	stack = croutine_stack_alloc(TEST_STACK_SIZE);
	if (check(stack != (struct croutine_stack *)-1,
			  "stack allocation for guard test should succeed") != 0)
		return -1;

	pid = fork();
	if (pid < 0) {
		croutine_stack_free(stack);
		return check(0, "fork should succeed");
	}

	if (pid == 0) {
		volatile unsigned char *guard;

		guard = (unsigned char *)stack->bottom - 1;
		*guard = 0xff;
		_exit(0);
	}

	if (waitpid(pid, &status, 0) != pid) {
		croutine_stack_free(stack);
		return check(0, "waitpid should succeed");
	}

	croutine_stack_free(stack);
	return check(WIFSIGNALED(status) && WTERMSIG(status) == SIGSEGV,
				 "writing guard page should terminate child with SIGSEGV");
}

int main(void) {
	if (test_invalid_size() != 0)
		return 1;
	if (test_stack_layout() != 0)
		return 1;
	if (test_guard_page() != 0)
		return 1;
	return 0;
}
