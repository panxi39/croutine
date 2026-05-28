#if defined(__aarch64__)

	.text
	.align 2
	.globl croutine_arch_store_and_call
	.type croutine_arch_store_and_call, %function

croutine_arch_store_and_call:
	.cfi_startproc
	.cfi_def_cfa sp, 0

	mov x2, sp
	str x2, [x0, #0]
	stp x19, x20, [x0, #8]
	stp x21, x22, [x0, #24]
	stp x23, x24, [x0, #40]
	stp x25, x26, [x0, #56]
	stp x27, x28, [x0, #72]
	stp x29, x30, [x0, #88]
	stp d8, d9, [x0, #104]
	stp d10, d11, [x0, #120]
	stp d12, d13, [x0, #136]
	stp d14, d15, [x0, #152]

	str x30, [sp, #-16]!
	blr x1
	ldr x30, [sp], #16

	ret

	.cfi_endproc
	.size croutine_arch_store_and_call, .-croutine_arch_store_and_call

	.globl croutine_arch_resume_and_ret
	.type croutine_arch_resume_and_ret, %function

croutine_arch_resume_and_ret:
	.cfi_startproc
	.cfi_def_cfa sp, 0

	ldr x2, [x0, #0]
	ldp x19, x20, [x0, #8]
	ldp x21, x22, [x0, #24]
	ldp x23, x24, [x0, #40]
	ldp x25, x26, [x0, #56]
	ldp x27, x28, [x0, #72]
	ldp x29, x30, [x0, #88]
	ldp d8, d9, [x0, #104]
	ldp d10, d11, [x0, #120]
	ldp d12, d13, [x0, #136]
	ldp d14, d15, [x0, #152]
	mov sp, x2

	ret

	.cfi_endproc
	.size croutine_arch_resume_and_ret, .-croutine_arch_resume_and_ret

	.section .note.GNU-stack,"",@progbits

#endif
