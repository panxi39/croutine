# Toolchain
CC         := clang
PKG_CONFIG ?= pkg-config

# Project layout
SRCDIR      := src
TESTDIR     := test
INCDIR      := include
BUILDDIR    := build
OBJDIR      := $(BUILDDIR)/obj
LIBDIR      := $(BUILDDIR)/lib
BUILDINCDIR := $(BUILDDIR)/include
TESTBINDIR  := $(BUILDDIR)/test

# Build parameters
CSTD     := c23
WARNINGS := -Wall -Wextra -pedantic
CPPFLAGS := -I$(INCDIR) -I$(SRCDIR)/include
CFLAGS   := $(WARNINGS) -std=$(CSTD) -fPIC -pthread $(CPPFLAGS) $(EXTRA_CFLAGS)
ASFLAGS  := -fPIC $(CPPFLAGS)
LDFLAGS  := $(EXTRA_LDFLAGS)
LDLIBS   :=

# External dependencies
DEPS := liburing

missing_pkg_config_dep = $(if $(shell $(PKG_CONFIG) --exists $(1) 2>/dev/null && echo yes),,$(1))
MISSING_DEPS := $(foreach dep,$(DEPS),$(call missing_pkg_config_dep,$(dep)))

ifneq ($(strip $(MISSING_DEPS)),)
$(error Missing pkg-config dependencies: $(MISSING_DEPS))
endif

CFLAGS  += $(shell $(PKG_CONFIG) --cflags $(DEPS))
LDFLAGS += $(shell $(PKG_CONFIG) --libs-only-L $(DEPS))
LDLIBS  += $(shell $(PKG_CONFIG) --libs-only-l $(DEPS))

# Architecture selection
HOST_ARCH := $(shell uname -m)

ifeq ($(HOST_ARCH),x86_64)
DETECTED_ARCH_IMPL := amd64
else ifeq ($(HOST_ARCH),amd64)
DETECTED_ARCH_IMPL := amd64
else ifeq ($(HOST_ARCH),aarch64)
DETECTED_ARCH_IMPL := aarch64
else ifeq ($(HOST_ARCH),arm64)
DETECTED_ARCH_IMPL := aarch64
else
$(error Unsupported architecture: $(HOST_ARCH))
endif

ARCH_IMPL ?= $(DETECTED_ARCH_IMPL)

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
TEST_CFLAGS := -pthread
TEST_LDLIBS := -pthread
TEST_ENV ?=
SANITIZE_FLAGS := -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: all lib test memtest $(TEST_NAMES) clean

all: lib

lib: $(LIB) $(BUILD_HEADERS)

test: $(TEST_BINS)
	@set -e; \
	for bin in $(TEST_BINS); do \
		$(TEST_ENV) ./$$bin; \
	done

memtest:
	$(MAKE) test BUILDDIR=$(BUILDDIR)/sanitize \
		EXTRA_CFLAGS="$(SANITIZE_FLAGS)" \
		EXTRA_LDFLAGS="$(SANITIZE_FLAGS)" \
		TEST_ENV="ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1"

$(TEST_NAMES): %: $(TESTBINDIR)/%
	./$<

clean:
	rm -rf $(BUILDDIR)

$(BUILDINCDIR)/%.h: $(INCDIR)/%.h
	mkdir -p $(@D)
	cp -f $< $@

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(PUBLIC_HEADERS) $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/lib.o: CFLAGS += -Wno-empty-translation-unit
$(OBJDIR)/entry.o: CFLAGS += -Wno-empty-translation-unit

$(OBJDIR)/%.o: $(SRCDIR)/%.S $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(ASFLAGS) -c -o $@ $<

$(LIB): $(LIBOBJ)
	mkdir -p $(@D)
	$(CC) -shared $(LDFLAGS) -Wl,-soname,$(LIB_NAME) -o $@ $^ $(LDLIBS)

$(TESTBINDIR)/%: $(TESTDIR)/%.c $(LIB) $(BUILD_HEADERS) $(PUBLIC_HEADERS) $(INTERNAL_HEADERS)
	mkdir -p $(@D)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(LDFLAGS) -o $@ $< -L$(LIBDIR) -lcroutine $(TEST_RPATH) $(LDLIBS) $(TEST_LDLIBS)
