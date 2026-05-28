ifeq ($(origin CC),default)
CC := clang
endif

PKG_CONFIG ?= pkg-config
TARGET     ?=
SYSROOT    ?=

TARGET_FLAGS  := $(if $(TARGET),--target=$(TARGET))
SYSROOT_FLAGS := $(if $(SYSROOT),--sysroot=$(SYSROOT))
TOOLCHAIN_FLAGS := $(TARGET_FLAGS) $(SYSROOT_FLAGS)

# Project layout
SRCDIR      := src
TESTDIR     := test
INCDIR      := include
BUILDDIR    := build
OBJDIR      := $(BUILDDIR)/obj
LIBDIR      := $(BUILDDIR)/lib
BUILDINCDIR := $(BUILDDIR)/include
TESTBINDIR  := $(BUILDDIR)/test

# Build flags
CSTD      := c23
WARNINGS  := -Wall -Wextra -pedantic
CPPFLAGS += -I$(INCDIR) -I$(SRCDIR)/include
THREAD_FLAGS := -pthread
CFLAGS   += $(WARNINGS) -std=$(CSTD) -fPIC $(THREAD_FLAGS) $(CPPFLAGS) $(EXTRA_CFLAGS)
ASFLAGS  += -fPIC $(CPPFLAGS) $(EXTRA_ASFLAGS)
LDFLAGS  += $(EXTRA_LDFLAGS)
LDLIBS   +=

# Optional pkg-config dependencies. Keep empty until a source file actually
# depends on an external package.
DEPS ?=

ifneq ($(strip $(DEPS)),)
missing_pkg_config_dep = $(if $(shell $(PKG_CONFIG) --exists $(1) 2>/dev/null && echo yes),,$(1))
MISSING_DEPS := $(foreach dep,$(DEPS),$(call missing_pkg_config_dep,$(dep)))

ifneq ($(strip $(MISSING_DEPS)),)
$(error Missing pkg-config dependencies: $(MISSING_DEPS))
endif

CFLAGS  += $(shell $(PKG_CONFIG) --cflags $(DEPS))
LDFLAGS += $(shell $(PKG_CONFIG) --libs-only-L $(DEPS))
LDLIBS  += $(shell $(PKG_CONFIG) --libs-only-l $(DEPS))
endif

# Architecture selection
MACHINE := $(if $(TARGET),$(firstword $(subst -, ,$(TARGET))),$(shell uname -m))

ifeq ($(MACHINE),x86_64)
DETECTED_ARCH_IMPL := amd64
else ifeq ($(MACHINE),amd64)
DETECTED_ARCH_IMPL := amd64
else ifeq ($(MACHINE),aarch64)
DETECTED_ARCH_IMPL := aarch64
else ifeq ($(MACHINE),arm64)
DETECTED_ARCH_IMPL := aarch64
else
$(error Unsupported architecture: $(MACHINE))
endif

ARCH      ?= $(DETECTED_ARCH_IMPL)
ARCH_IMPL ?= $(ARCH)

# Library
LIB_NAME       := libcroutine.so
LIB            := $(LIBDIR)/$(LIB_NAME)
PUBLIC_HEADERS   := $(wildcard $(INCDIR)/*.h)
BUILD_HEADERS    := $(patsubst $(INCDIR)/%,$(BUILDINCDIR)/%,$(PUBLIC_HEADERS))
INTERNAL_HEADERS := $(wildcard $(SRCDIR)/include/*.h) $(wildcard $(SRCDIR)/arch/*.h)

LIBCSRC := $(wildcard $(SRCDIR)/*.c) $(SRCDIR)/arch/$(ARCH_IMPL)_context.c
LIBASRC := $(SRCDIR)/arch/$(ARCH_IMPL)_switch.S
LIBOBJ  := $(patsubst $(SRCDIR)/%.c,$(OBJDIR)/%.o,$(LIBCSRC))
LIBOBJ  += $(patsubst $(SRCDIR)/%.S,$(OBJDIR)/%.o,$(LIBASRC))

# Tests
TEST_FILES := structures.c scheduler.c runtime.c
TEST_NAMES := $(basename $(TEST_FILES))
TEST_BINS  := $(addprefix $(TESTBINDIR)/,$(TEST_NAMES))
TEST_RPATH := -Wl,-rpath,'$$ORIGIN/../lib'
TEST_CFLAGS :=
TEST_LDLIBS := $(THREAD_FLAGS)
TEST_ENV ?=
RUNNER ?=
SANITIZE_FLAGS := -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: all lib test memtest $(TEST_NAMES) clean

all: lib

lib: $(LIB) $(BUILD_HEADERS)

test: $(TEST_BINS)
	@set -e; \
	for bin in $(TEST_BINS); do \
		$(TEST_ENV) $(RUNNER) ./$$bin; \
	done

memtest:
	$(MAKE) test BUILDDIR=$(BUILDDIR)/sanitize \
		EXTRA_CFLAGS="$(SANITIZE_FLAGS)" \
		EXTRA_LDFLAGS="$(SANITIZE_FLAGS)" \
		TEST_ENV="ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1"

$(TEST_NAMES): %: $(TESTBINDIR)/%
	$(TEST_ENV) $(RUNNER) ./$<

clean:
	rm -rf $(BUILDDIR)

$(BUILDINCDIR)/%.h: $(INCDIR)/%.h
	mkdir -p $(@D)
	cp -f $< $@

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(PUBLIC_HEADERS) $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(TOOLCHAIN_FLAGS) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/lib.o: CFLAGS += -Wno-empty-translation-unit
$(OBJDIR)/entry.o: CFLAGS += -Wno-empty-translation-unit

$(OBJDIR)/%.o: $(SRCDIR)/%.S $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(TOOLCHAIN_FLAGS) $(ASFLAGS) -c -o $@ $<

$(LIB): $(LIBOBJ)
	mkdir -p $(@D)
	$(CC) $(TOOLCHAIN_FLAGS) -shared $(LDFLAGS) -Wl,-soname,$(LIB_NAME) -o $@ $^ $(LDLIBS)

$(TESTBINDIR)/%: $(TESTDIR)/%.c $(LIB) $(BUILD_HEADERS) $(PUBLIC_HEADERS) $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(TOOLCHAIN_FLAGS) $(CFLAGS) $(TEST_CFLAGS) $(LDFLAGS) -o $@ $< -L$(LIBDIR) -lcroutine $(TEST_RPATH) $(LDLIBS) $(TEST_LDLIBS)
