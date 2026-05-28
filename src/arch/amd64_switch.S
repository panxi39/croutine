#if defined(__x86_64__) || defined(__amd64__)

	.text
	.globl croutine_arch_store_and_call
	.type croutine_arch_store_and_call, @function

croutine_arch_store_and_call:
	.cfi_startproc
	.cfi_def_cfa %rsp, 8
	.cfi_offset %rip, -8

	movq %rsp, 0(%rdi)
	movq %rbx, 8(%rdi)
	movq %rbp, 16(%rdi)
	movq %r12, 24(%rdi)
	movq %r13, 32(%rdi)
	movq %r14, 40(%rdi)
	movq %r15, 48(%rdi)

	subq $8, %rsp
	call *%rsi
	addq $8, %rsp

	ret

	.cfi_endproc
	.size croutine_arch_store_and_call, .-croutine_arch_store_and_call

	.globl croutine_arch_resume_and_ret
	.type croutine_arch_resume_and_ret, @function

croutine_arch_resume_and_ret:
	.cfi_startproc
	.cfi_def_cfa %rsp, 8
	.cfi_offset %rip, -8

	movq 8(%rdi), %rbx
	movq 16(%rdi), %rbp
	movq 24(%rdi), %r12
	movq 32(%rdi), %r13
	movq 40(%rdi), %r14
	movq 48(%rdi), %r15
	movq 0(%rdi), %rsp

	ret

	.cfi_endproc
	.size croutine_arch_resume_and_ret, .-croutine_arch_resume_and_ret

	.section .note.GNU-stack,"",@progbits

#endif
