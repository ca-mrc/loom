# Guest runtime payload

This Linux/amd64 build assembles the userspace and kernel files needed by the
trial guest launcher. It contains no task input, model adapter, credentials,
initramfs, or Loom executable. The runtime image adds its own static guest-init
and sandbox binaries and constructs the initramfs.

Build and verify from the repository root:

```sh
docker build -f deploy/Dockerfile.execution-runtime --target guest-payload -t loom-guest-payload:local .
```

The `guest-payload` stage is a scratch image containing `/payload`. Its build
depends on the `guest-verify` stage, which runs as UID/GID 65532 in scratch, with no
system libc or shell. Verification checks every file checksum, runs QEMU and its
9p device support, formats a real ext4 file, resolves guest kernel module
dependencies, and invokes the Docker binaries. It does not start a Docker daemon
or establish task/guest isolation; those require the runtime integration tests.

`sources.lock.json` pins the builder/resolver image digests and all downloaded
archive sizes and SHA256 hashes. Debian package hashes came from APT's verified
Ubuntu package indexes. Package versions, including dependencies, are fixed;
the builder does not resolve current APT candidates. It downloads those files
over HTTPS, checks their contents and package metadata, and extracts them
without executing package installation hooks. An unavailable pinned archive
fails the build. Updating dependencies requires updating the lock and running
the full payload verification and guest capability tests.

The runtime build uses Go 1.26.10, static Docker 29.9.2 and Buildx 0.38.1 from
the digest-pinned official Docker CLI image. These releases replace older Go
stdlib versions rejected by the release scanner. The Go advisory for
[CVE-2026-56857](https://pkg.go.dev/vuln/GO-2026-6604) describes Windows junction
handling; the existing release scan still rejects the affected version matches
in Linux binaries without exceptions. Updating the compiler and official tools
retains that policy. The kernel, Ubuntu package set, guest paths, unprivileged
verification and isolation requirements are unchanged.

The service image also copies official Trivy 0.76.1 and kubectl 1.36.6 binaries
to replace affected embedded Go libraries, keeping kubectl on the same minor
release. Alpine 3.23 `brotli-libs=1.2.1-r0` fixes the additional Brotli
CRITICAL finding in the service base image. This does not change the separately
installed release scanner, its
CRITICAL threshold, or the no-exceptions policy. The unchanged Trivy 0.74.0
release scanner qualifies these patched binaries and the execution-runtime
archive; guest runtime integration remains a separate requirement.

For HTTP 500, 502, 503 or 504, a connection/TLS-handshake or response-body
timeout, a connection reset (including a remote disconnect without a response),
an unexpected TLS EOF, or an explicit incomplete-read error while reading the
body, the downloader shares one budget of at most three attempts, waiting
one and then two seconds between attempts. Each attempt retains the 120-second
network timeout; an interrupted body restarts from byte zero. Diagnostics
identify the locked archive by SHA256, HTTP status or connection/body interruption
category, and attempt count, without logging URLs or response bodies.
Certificate-validation errors, other TLS errors, other HTTP errors, other transport failures,
local filesystem failures and size/hash mismatches fail without
retries. Failed downloads remove their partial files; only complete,
verified archives enter the cache. These are build-time archive retries, not
task execution retries.

`archive.ubuntu.com` removes a package file from `pool/` once Ubuntu publishes
a newer version, so a pinned URL there starts returning 404. Launchpad's
Librarian keeps every published file permanently but serves the payload's
packages several times more slowly, enough to push in-test image builds past
their timeouts. Each package entry therefore has two sources for the same
bytes: `url`, its `archive.ubuntu.com` pool file, and `fallback_url`, its
permanent Librarian file. The downloader uses the fallback when the mirror
returns 404, or exhausts its three attempts on HTTP 500, 502, 503 or 504.
Exhausted transport errors fail without trying the fallback. Both sources pass
the same size and SHA256 checks. Each Librarian URL was resolved through
Launchpad's official `+files` endpoint
(`https://launchpad.net/ubuntu/+archive/primary/+files/<file>.deb`) and its
downloaded bytes were verified against the locked size and SHA256; the direct
file URL avoids that endpoint's redirect, which previously timed out or returned
502 in CI. Ubuntu snapshot URLs also returned 500/502/503 during builds. When
adding or updating a package, record both URLs:
`tests/ops/test_guest_payload_download.py` requires the pair and that both name
the entry's exact name, version and architecture.

The output is relocatable and contains regular files rather than absolute
distro symlinks. `SHA256SUMS` covers all other payload files; `sources.lock.json`
retains the inputs and `versions.json` records executable versions. Files are
readable by an unprivileged launcher, including the distribution kernel.

| Path under `/payload` | Purpose |
| --- | --- |
| `bin/qemu-system-x86_64`, `lib/`, `lib/qemu/` | QEMU, dynamic loader and complete linked library/module closure |
| `share/qemu/`, `share/seabios/`, `share/ipxe/` | Firmware and network ROMs, including dereferenced distro links |
| `bin/mke2fs`, `etc/mke2fs.conf` | Ext4 state-disk creation |
| `kernel`, `kernel.config`, `kernel-release` | Pinned Ubuntu kernel image, configuration and release |
| `modules/<release>/` | Entire base kernel module package, decompressed, with regenerated dependency indexes |
| `boot-modules/` | Uncompressed netfs, 9pnet, 9pnet_virtio, 9p and overlay; `load-order` gives the initialization order |
| `bin/busybox` | Static shell, mount, ip, insmod, modprobe and other init utilities |
| `bin/kmod` | Full modprobe/depmod implementation for optional diagnostic use |
| `docker/` | Pinned static Docker CLI, daemon, containerd, shims, runc and proxy |
| `docker/cli-plugins/docker-buildx` | Checksum-verified Buildx plugin from a digest-pinned official Docker CLI image |
| `bin/xtables-*-multi`, `lib/xtables/` | iptables/ip6tables helpers and extension libraries for guest Docker networking |
| `licenses/` | Copyright notices from the pinned Debian packages |

The kernel base module package includes 9p, overlay, veth, bridge and netfilter
support. The optional Ubuntu `linux-modules-extra` driver package is not included.
No host kernel modules are loaded during packaging or its scratch verification.
BusyBox modprobe handles ordinary loading after the guest exposes the module
tree as `/lib/modules`; its Ubuntu build does not implement kmod's `-d`, `-S`
or dry-run flags. Use the bundled kmod binary for those options.

Dynamic binaries must use the bundled loader; direct execution would search the
task image's loader/libc and can fail on old or minimal task distributions:

```sh
payload=/opt/loom-guest
export QEMU_MODULE_DIR="$payload/lib/qemu"
"$payload/lib/ld-linux-x86-64.so.2" --inhibit-cache \
  --library-path "$payload/lib" "$payload/bin/qemu-system-x86_64" \
  -L "$payload/share/qemu" -bios "$payload/share/seabios/bios-256k.bin" --version

MKE2FS_CONFIG="$payload/etc/mke2fs.conf" \
  "$payload/lib/ld-linux-x86-64.so.2" --inhibit-cache \
  --library-path "$payload/lib" "$payload/bin/mke2fs" -t ext4 -F state.ext4
```

The Docker binaries are static, but normal daemon bridge networking still needs
the bundled iptables helpers. Guest initialization must provide `iptables` and
`ip6tables` command wrappers/aliases that invoke `xtables-legacy-multi` or
`xtables-nft-multi` through the loader with the appropriate `--argv0`, and set
`XTABLES_LIBDIR` to the guest path of `lib/xtables`. Daemon sockets, data roots,
networking and cache retention remain the guest runtime's responsibility.
Expose the bundled `docker/cli-plugins` directory at a Docker CLI plugin search
path (for example `$DOCKER_CONFIG/cli-plugins`) so ordinary `docker build` can
use BuildKit without relying on the deprecated legacy builder.
