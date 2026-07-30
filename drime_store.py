#!/usr/bin/env python3
# drime_store.py

import os
import re
import sys
import json
import time
import uuid
import gzip
import sqlite3
import hashlib
import threading
import subprocess
from pathlib import Path


def parse_size(v):
    if isinstance(v, int):
        return int(v)

    s = str(v).strip().upper()
    m = re.match(r'^([0-9]*\.?[0-9]+)([KMGTP]?I?B?)$', s)

    if not m:
        raise ValueError(f"bad size: {v}")

    num = float(m.group(1))
    unit = m.group(2)

    mult = 1

    if unit.startswith("K"):
        mult = 1024
    elif unit.startswith("M"):
        mult = 1024 ** 2
    elif unit.startswith("G"):
        mult = 1024 ** 3
    elif unit.startswith("T"):
        mult = 1024 ** 4
    elif unit.startswith("P"):
        mult = 1024 ** 5

    return int(num * mult)


def sha256_file(path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)

    return h.hexdigest()


def run_rclone(args, check=True):
    cmd = ["rclone"] + args

    p = subprocess.run(
        cmd,
        text=True,
        capture_output=True
    )

    if check and p.returncode != 0:
        raise subprocess.CalledProcessError(
            p.returncode,
            cmd,
            output=p.stdout,
            stderr=p.stderr
        )

    return p


class DrimeStore:
    def __init__(
        self,
        remote,
        cache,
        size="20G",
        chunk_size="4M",
        sync_mode="writeback"
    ):
        self.remote = remote.strip().rstrip("/")
        self.cache = Path(cache).expanduser().resolve()

        self.chunks_dir = self.cache / "chunks"
        self.tmp_dir = self.cache / "tmp"
        self.db_path = self.cache / "meta.db"

        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        self.size = parse_size(size)
        self.chunk_size = parse_size(chunk_size)
        self.sync_mode = sync_mode

        self.lock = threading.RLock()

        self.db = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False
        )

        self.db.execute("PRAGMA journal_mode=WAL")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                sha256 TEXT,
                uploaded INTEGER DEFAULT 0,
                dirty INTEGER DEFAULT 0,
                gen INTEGER DEFAULT 0,
                updated REAL
            )
        """)

        self.db.commit()

        self.dirty = set()
        self._load_dirty()

    def _load_dirty(self):
        cur = self.db.execute("""
            SELECT id FROM chunks
            WHERE dirty = 1 OR uploaded = 0
        """)

        for row in cur.fetchall():
            self.dirty.add(int(row[0]))

    def set_meta(self, k, v):
        self.db.execute("""
            INSERT INTO meta(k, v)
            VALUES(?, ?)
            ON CONFLICT(k) DO UPDATE SET v = excluded.v
        """, (k, str(v)))

        self.db.commit()

    def get_meta(self, k):
        cur = self.db.execute("""
            SELECT v FROM meta WHERE k = ?
        """, (k,))

        row = cur.fetchone()

        if row is None:
            return None

        return row[0]

    @property
    def chunk_count(self):
        return (self.size + self.chunk_size - 1) // self.chunk_size

    def chunk_len(self, cid):
        start = cid * self.chunk_size
        return min(self.chunk_size, self.size - start)

    def chunk_path(self, cid):
        return self.chunks_dir / f"{cid:08d}.part"

    def _atomic_write(self, path, data):
        tmp = self.tmp_dir / f"{path.name}.{uuid.uuid4().hex}"

        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp, path)

    def init(self):
        with self.lock:
            self.set_meta("size", self.size)
            self.set_meta("chunk_size", self.chunk_size)
            self.set_meta("created", time.time())
            self.upload_manifest()

    def fsck(self):
        with self.lock:
            self.set_meta("size", self.size)
            self.set_meta("chunk_size", self.chunk_size)

            cur = self.db.execute("SELECT COUNT(*) FROM chunks")
            count = cur.fetchone()[0]

            if count == 0:
                self.load_remote_manifest()

            cur = self.db.execute("""
                SELECT id FROM chunks
                WHERE dirty = 1 OR uploaded = 0
            """)

            for row in cur.fetchall():
                cid = int(row[0])
                p = self.chunk_path(cid)

                if p.exists():
                    self.sync_chunk(cid)

            self.upload_manifest()

    def load_remote_manifest(self):
        tmp = self.tmp_dir / f"manifest.{uuid.uuid4().hex}.gz"

        try:
            run_rclone([
                "copyto",
                f"{self.remote}/manifest.json.gz",
                str(tmp)
            ])

            with gzip.open(tmp, "rt") as f:
                m = json.load(f)

            if int(m.get("size", 0)) != self.size:
                raise ValueError("remote manifest size mismatch")

            if int(m.get("chunk_size", 0)) != self.chunk_size:
                raise ValueError("remote manifest chunk_size mismatch")

            for c in m.get("chunks", []):
                self.db.execute("""
                    INSERT INTO chunks(
                        id,
                        sha256,
                        uploaded,
                        dirty,
                        gen,
                        updated
                    )
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        sha256 = excluded.sha256,
                        uploaded = 1,
                        dirty = 0,
                        gen = excluded.gen,
                        updated = excluded.updated
                """, (
                    int(c["id"]),
                    c.get("sha256", ""),
                    1,
                    0,
                    int(c.get("gen", 0)),
                    float(c.get("updated", time.time()))
                ))

            self.db.commit()

        except subprocess.CalledProcessError:
            pass

        finally:
            try:
                tmp.unlink()
            except Exception:
                pass

    def upload_manifest(self):
        cur = self.db.execute("""
            SELECT id, sha256, uploaded, dirty, gen, updated
            FROM chunks
            ORDER BY id
        """)

        chunks = []

        for row in cur.fetchall():
            chunks.append({
                "id": int(row[0]),
                "sha256": row[1],
                "uploaded": int(row[2]),
                "dirty": int(row[3]),
                "gen": int(row[4]),
                "updated": float(row[5]) if row[5] else 0.0
            })

        manifest = {
            "version": 1,
            "remote": self.remote,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "generated": time.time(),
            "chunks": chunks
        }

        tmp = self.tmp_dir / f"manifest.{uuid.uuid4().hex}.gz"

        with gzip.open(tmp, "wt") as f:
            json.dump(manifest, f)

        run_rclone([
            "copyto",
            str(tmp),
            f"{self.remote}/manifest.json.gz"
        ])

        try:
            tmp.unlink()
        except Exception:
            pass

    def read_chunk(self, cid):
        with self.lock:
            p = self.chunk_path(cid)

            if p.exists():
                return p.read_bytes()

            tmp = self.tmp_dir / f"{cid:08d}.{uuid.uuid4().hex}.gz"

            try:
                run_rclone([
                    "copyto",
                    f"{self.remote}/chunks/{cid:08d}.gz",
                    str(tmp)
                ])

                with gzip.open(tmp, "rb") as f:
                    data = f.read()

                expected = self.chunk_len(cid)

                if len(data) < expected:
                    data += b"\x00" * (expected - len(data))

                self._atomic_write(p, data)

                return data

            except subprocess.CalledProcessError as e:
                err = (e.stderr or "").lower()

                if (
                    "not found" in err or
                    "object not found" in err or
                    "doesn't exist" in err or
                    "file not found" in err
                ):
                    return b"\x00" * self.chunk_len(cid)

                raise

            finally:
                try:
                    tmp.unlink()
                except Exception:
                    pass

    def write_chunk(self, cid, data, offset_in_chunk):
        with self.lock:
            p = self.chunk_path(cid)

            if p.exists():
                chunk = bytearray(p.read_bytes())
            else:
                chunk = bytearray(self.read_chunk(cid))

            expected = self.chunk_len(cid)

            if len(chunk) < expected:
                chunk.extend(b"\x00" * (expected - len(chunk)))

            chunk[offset_in_chunk:offset_in_chunk + len(data)] = data

            final = bytes(chunk)

            self._atomic_write(p, final)

            sha = hashlib.sha256(final).hexdigest()

            self.db.execute("""
                INSERT INTO chunks(
                    id,
                    sha256,
                    uploaded,
                    dirty,
                    gen,
                    updated
                )
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    sha256 = excluded.sha256,
                    uploaded = 0,
                    dirty = 1,
                    gen = gen + 1,
                    updated = excluded.updated
            """, (
                cid,
                sha,
                0,
                1,
                0,
                time.time()
            ))

            self.db.commit()

            self.dirty.add(cid)

            if self.sync_mode == "writethrough":
                self.sync_chunk(cid)

    def sync_chunk(self, cid):
        with self.lock:
            p = self.chunk_path(cid)

            if not p.exists():
                return

            data = p.read_bytes()
            sha = hashlib.sha256(data).hexdigest()

            gztmp = self.tmp_dir / f"{cid:08d}.{uuid.uuid4().hex}.gz"

            with gzip.open(gztmp, "wb") as f:
                f.write(data)

            run_rclone([
                "copyto",
                str(gztmp),
                f"{self.remote}/chunks/{cid:08d}.gz"
            ])

            try:
                gztmp.unlink()
            except Exception:
                pass

            self.db.execute("""
                UPDATE chunks
                SET
                    uploaded = 1,
                    dirty = 0,
                    sha256 = ?,
                    updated = ?
                WHERE id = ?
            """, (
                sha,
                time.time(),
                cid
            ))

            self.db.commit()

            self.dirty.discard(cid)

    def flush(self):
        with self.lock:
            for cid in sorted(self.dirty):
                self.sync_chunk(cid)

            self.upload_manifest()

    def read(self, offset, length):
        out = bytearray(length)
        pos = 0

        while pos < length:
            off = offset + pos
            cid = off // self.chunk_size
            coff = off % self.chunk_size

            n = min(
                self.chunk_size - coff,
                length - pos
            )

            chunk = self.read_chunk(cid)

            out[pos:pos + n] = chunk[coff:coff + n]

            pos += n

        return bytes(out)

    def write(self, data, offset):
        pos = 0
        total = len(data)

        while pos < total:
            off = offset + pos
            cid = off // self.chunk_size
            coff = off % self.chunk_size

            n = min(
                self.chunk_size - coff,
                total - pos
            )

            self.write_chunk(
                cid,
                data[pos:pos + n],
                coff
            )

            pos += n
