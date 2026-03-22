"""
core/persistence.py  —  PolyBot v7.1 FINAL  (cloud-hardened)

Changes vs previous version:
  - Auto-creates data/ directory AND the JSON file on every init
  - Every file IO path wrapped in try/except — never raises to caller
  - Corrupt / non-dict file: quarantine then return {} (never crash)
  - Missing file: silently create empty file then return {}
  - _atomic_write: falls back to direct write if temp-file fails
    (handles read-only-parent edge-case on some container mounts)
  - Ephemeral-environment safe (Fly.io, Railway, Render)
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path

log = logging.getLogger("polybot.persistence")

_LIVE_STATES = {"PLACED", "PARTIAL_FILL", "FILLED"}


class PersistenceManager:

    def __init__(
        self,
        state_file: str = "data/active_trades.json",
        stale_ttl:  float = 300.0,
    ) -> None:
        self.path      = Path(state_file)
        self.stale_ttl = stale_ttl
        self._ensure_storage()

    # ── Public API ─────────────────────────────────────────────────────────────

    def load(self) -> dict[str, dict]:
        """
        Load active trades from disk.
        Returns only LIVE state entries after evicting stale ones.
        NEVER raises — returns {} on any error.
        """
        self._ensure_storage()

        if not self.path.exists():
            log.info("Persistence: state file not found — fresh start")
            return {}

        try:
            text = self.path.read_text(encoding="utf-8").strip()
            if not text:
                return {}
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            log.error("Persistence: corrupt JSON in %s: %s — quarantining", self.path, exc)
            self._quarantine()
            return {}
        except Exception as exc:
            log.error("Persistence: read error %s: %s — fresh start", self.path, exc)
            return {}

        if not isinstance(raw, dict):
            log.error("Persistence: expected dict in %s, got %s — quarantining", self.path, type(raw).__name__)
            self._quarantine()
            return {}

        now    = time.time()
        cutoff = now - self.stale_ttl
        kept:  dict[str, dict] = {}

        for tid, entry in raw.items():
            try:
                if not isinstance(entry, dict):
                    continue
                if entry.get("state") not in _LIVE_STATES:
                    continue
                ts = float(entry.get("timestamp", 0))
                if ts < cutoff:
                    log.debug("Persistence: evicting stale trade_id=%s age=%.0fs", tid, now - ts)
                    continue
                kept[tid] = entry
            except Exception as exc:
                log.warning("Persistence: skipping malformed entry %s: %s", tid, exc)

        log.info("Persistence: loaded %d live trade(s) from %s", len(kept), self.path)
        return kept

    def save(self, active_trades: dict[str, dict]) -> None:
        """
        Atomically persist active trades to disk.
        Filters to LIVE states only.
        NEVER raises.
        """
        try:
            to_write = {
                tid: entry
                for tid, entry in active_trades.items()
                if isinstance(entry, dict) and entry.get("state") in _LIVE_STATES
            }
            self._atomic_write(to_write)
        except Exception as exc:
            log.error("Persistence: save outer error: %s", exc)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _ensure_storage(self) -> None:
        """Create data directory and empty state file if either is missing."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            log.warning("Persistence: cannot create dir %s: %s", self.path.parent, exc)

        if not self.path.exists():
            try:
                self.path.write_text("{}", encoding="utf-8")
                log.info("Persistence: created empty state file %s", self.path)
            except Exception as exc:
                log.warning("Persistence: cannot create state file %s: %s", self.path, exc)

    def _atomic_write(self, data: dict) -> None:
        """
        Write data atomically: temp-file + fsync + os.replace.
        Falls back to direct write if the temp approach fails
        (e.g. temp dir on different filesystem on some container configs).
        """
        try:
            fd, tmp = tempfile.mkstemp(
                dir=self.path.parent, prefix=".~trades_", suffix=".json"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, self.path)
                log.debug("Persistence: saved %d entries (atomic)", len(data))
                return
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except Exception as exc:
            log.warning("Persistence: atomic write failed (%s) — trying direct write", exc)
            try:
                self.path.write_text(
                    json.dumps(data, indent=2), encoding="utf-8"
                )
                log.debug("Persistence: saved %d entries (direct fallback)", len(data))
            except Exception as exc2:
                log.error("Persistence: ALL writes failed: %s", exc2)

    def _quarantine(self) -> None:
        """Rename corrupt file so it doesn't block future starts."""
        try:
            dst = self.path.with_suffix(f".corrupt_{int(time.time())}.json")
            os.rename(self.path, dst)
            log.warning("Persistence: quarantined corrupt file -> %s", dst.name)
            # Re-create a clean empty file so the next save has somewhere to write
            self.path.write_text("{}", encoding="utf-8")
        except Exception as exc:
            log.error("Persistence: quarantine failed: %s", exc)
