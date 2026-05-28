#ifndef CROUTINE_HELPERS_H
#define CROUTINE_HELPERS_H

#include <stddef.h>

#define croutine_container_of(ptr, type, member)                              \
	__extension__({                                                           \
		void *__mptr = (void *)(ptr);                                         \
		static_assert(__builtin_types_compatible_p(                           \
						  typeof(*(ptr)), typeof(((type *)0)->member)) ||     \
						  __builtin_types_compatible_p(typeof(*(ptr)), void), \
					  "pointer type mismatch in container_of()");             \
		((type *)(__mptr - offsetof(type, member)));                          \
	})

#endif
