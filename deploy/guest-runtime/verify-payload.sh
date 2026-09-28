#!/bin/sh
# Run in the scratch verification stage: no host interpreter or libraries exist.
set -eu
export PATH=/payload/bin
export QEMU_MODULE_DIR=/payload/lib/qemu

fail() { echo "guest payload: $*" >&2; exit 1; }
for tool in busybox qemu-system-x86_64 mke2fs; do
    [ -x "/payload/bin/$tool" ] || fail "missing executable $tool"
done
[ -x /payload/lib/ld-linux-x86-64.so.2 ] || fail 'missing ELF loader'
cd /payload
busybox sha256sum -c SHA256SUMS >/dev/null

run_dynamic() {
    /payload/lib/ld-linux-x86-64.so.2 --inhibit-cache \
        --library-path /payload/lib "$@"
}

run_dynamic /payload/bin/qemu-system-x86_64 --version
run_dynamic /payload/bin/qemu-system-x86_64 \
    -L /payload/share/qemu -device virtio-9p-pci,help >/dev/null
busybox mkdir -p /tmp
busybox truncate -s 32M /tmp/check.ext4
MKE2FS_CONFIG=/payload/etc/mke2fs.conf \
    run_dynamic /payload/bin/mke2fs -q -F -t ext4 /tmp/check.ext4
busybox rm /tmp/check.ext4

busybox ip link show lo
release=$(busybox cat /payload/kernel-release)
busybox mkdir -p /tmp/modules/lib
busybox ln -s /payload/modules /tmp/modules/lib/modules
for module in overlay veth iptable_nat; do
    /payload/lib/ld-linux-x86-64.so.2 --inhibit-cache \
        --library-path /payload/lib --argv0 modprobe /payload/bin/kmod \
        -d /tmp/modules -S "$release" --show-depends "$module"
done
for module in netfs 9pnet 9pnet_virtio 9p overlay; do
    [ -s "/payload/boot-modules/$module.ko" ] || fail "missing boot module $module"
done
[ -s /payload/kernel ] || fail 'missing kernel'
[ -s /payload/share/seabios/bios-256k.bin ] || fail 'missing BIOS'

for tool in docker dockerd containerd runc; do
    "/payload/docker/$tool" --version
done
busybox mkdir -p /tmp/docker-config
busybox ln -s /payload/docker/cli-plugins /tmp/docker-config/cli-plugins
DOCKER_CONFIG=/tmp/docker-config /payload/docker/docker buildx version
XTABLES_LIBDIR=/payload/lib/xtables \
    /payload/lib/ld-linux-x86-64.so.2 --inhibit-cache \
        --library-path /payload/lib --argv0 iptables \
        /payload/bin/xtables-legacy-multi --version
echo 'guest payload verification passed'
