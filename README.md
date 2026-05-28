> [!WARNING]
> This project is experimental and incomplete. Do not use it in production.

<h1 align="center"><samp>croutine</samp></h1>

<p align="center">
  <samp>An experimental, event-source-agnostic coroutine library for C.</samp>
</p>

## Make Commands

- `make`: Build the default target (`lib`).
- `make all`: Build the library.
- `make lib`: Build `build/lib/libcroutine.so` and stage public headers under `build/include/`.
- `make test`: Build and run all configured tests.
- `make memtest`: Build and run all tests with sanitizer flags enabled.
- `make clean`: Remove generated build output under `build/`.
