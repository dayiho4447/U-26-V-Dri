#!/usr/bin/env python3
# drime_nbd.py

import nbdkit
from drime_store import DrimeStore

API_VERSION = 2

CFG = {}
STORE = None


def config(key, value):
    global CFG
    CFG[key] = value


def config_complete():
    global STORE

    remote = CFG.get("remote")
    cache = CFG.get("cache", "/var/lib/drimelive")
    size = CFG.get("size", "20G")
    chunk = CFG.get("chunk", "4M")
    sync = CFG.get("sync", "writeback")

    if not remote:
        raise ValueError("remote is required")

    STORE = DrimeStore(
        remote=remote,
        cache=cache,
        size=size,
        chunk_size=chunk,
        sync_mode=sync
    )

    STORE.fsck()


def open(readonly):
    return STORE


def close(h):
    try:
        h.flush()
    except Exception:
        pass


def get_size(h):
    return h.size


def can_write(h):
    return True


def can_flush(h):
    return True


def pread(h, buf, offset, flags):
    data = h.read(offset, len(buf))
    buf[:len(data)] = data
    return buf


def pwrite(h, buf, offset, flags):
    h.write(bytes(buf), offset)
    return len(buf)


def flush(h):
    h.flush()
