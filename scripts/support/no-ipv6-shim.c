/*
 * TEST-ENVIRONMENT WORKAROUND ONLY. Not used by the relay.
 *
 * Holochain 0.7.1's websocket listener always dual-binds IPv6 + IPv4 and only
 * falls back to IPv4 when binding IPv6 fails with EADDRNOTAVAIL. On kernels
 * built without IPv6, socket(AF_INET6, ...) fails with EAFNOSUPPORT instead,
 * and the conductor aborts at startup. This LD_PRELOAD shim maps that errno
 * so Holochain's own fallback path is taken. See docs/holochain-integration.md.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <sys/socket.h>

int socket(int domain, int type, int protocol) {
    static int (*real_socket)(int, int, int) = 0;
    if (!real_socket) {
        real_socket = (int (*)(int, int, int))dlsym(RTLD_NEXT, "socket");
    }
    int fd = real_socket(domain, type, protocol);
    if (fd < 0 && domain == AF_INET6 && errno == EAFNOSUPPORT) {
        errno = EADDRNOTAVAIL;
    }
    return fd;
}
