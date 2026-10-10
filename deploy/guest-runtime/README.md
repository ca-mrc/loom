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

For HTTP 502, 503 or 504, the downloader makes at most three attempts, waiting
one and then two seconds between attempts. Each attempt retains the 120-second
network timeout. Diagnostics identify the locked archive by SHA256, HTTP status
and attempt count, without logging URLs or response bodies. Other HTTP errors,
transport failures, local filesystem failures and size/hash mismatches fail
without retries. Failed downloads remove their partial files; only complete,
verified archives enter the cache. These are build-time archive retries, not
task execution retries.

`archive.ubuntu.com` removes a package file from `pool/` once Ubuntu publishes
a newer version, so a pinned URL there can start returning 404. The `libssl3t64`
and `libpng16-16t64` entries use the [Ubuntu snapshot service](https://snapshot.ubuntu.com/) at
`20260928T000000Z`, before the pinned versions were superseded. Launchpad's
`+files` redirect timed out or returned 502 during image and Docker CI builds.
The snapshot serves the same version, size and hash, so the extracted payload
files are unchanged; only the retained `sources.lock.json` differs. Ubuntu
currently guarantees snapshot availability for at least two years; refresh the
snapshot together with the dependency lock before that retention boundary.

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
