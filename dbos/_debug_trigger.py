"""
Debug trigger utility, for inserting code to replicate specific timing sequences, mock error conditions, etc.
"""

from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from threading import Lock, Semaphore
from typing import Callable, Dict, Optional


@dataclass(frozen=True)
class CallSiteInfo:
    file_name: str
    line_number: int


@dataclass
class DebugAction:
    sleep_ms: Optional[int] = None
    callback: Optional[Callable[[], None]] = None
    semaphore: Optional[Semaphore] = None
    exception_to_throw: Optional[BaseException] = None
    _lock: Lock = field(default_factory=Lock, repr=False)

    def set_sleep_ms(self, sleep_ms: Optional[int]) -> "DebugAction":
        self.sleep_ms = sleep_ms
        return self

    def set_callback(self, callback: Optional[Callable[[], None]]) -> "DebugAction":
        self.callback = callback
        return self

    def set_semaphore(self, semaphore: Optional[Semaphore]) -> "DebugAction":
        self.semaphore = semaphore
        return self

    def set_exception_to_throw(self, exc: Optional[BaseException]) -> "DebugAction":
        self.exception_to_throw = exc
        return self

    def pop_exception_once(self) -> Optional[BaseException]:
        """Return and clear the exception atomically."""
        with self._lock:
            exc = self.exception_to_throw
            self.exception_to_throw = None
            return exc


class DebugTriggers:
    # ----- Constants (Should use in just one place in the code) -----
    DEBUG_TRIGGER_STEP_COMMIT = "DEBUG_TRIGGER_STEP_COMMIT"
    DEBUG_TRIGGER_INITWF_COMMIT = "DEBUG_TRIGGER_INITWF_COMMIT"

    # Internal registries
    _point_triggers: Dict[str, DebugAction] = {}
    _triggers_lock = Lock()

    @classmethod
    def debug_trigger_point(cls, name: str) -> None:
        """
        Proceed according to the configured DebugAction.

        Order:
          1) sleep (if configured)
          2) run callback (if configured)
          3) raise exception once (if configured)
          4) acquire semaphore (if configured) and block until released
        """

        # get current action (no lock held during action execution)
        with cls._triggers_lock:
            action = cls._point_triggers.get(name)

        if action is None:
            return

        if action.sleep_ms is not None and action.sleep_ms > 0:
            time.sleep(action.sleep_ms / 1000.0)

        if action.callback is not None:
            action.callback()

        exc = action.pop_exception_once()
        if exc is not None:
            raise exc

        if action.semaphore is not None:
            action.semaphore.acquire()

    @classmethod
    def set_debug_trigger(cls, name: str, action: DebugAction) -> None:
        with cls._triggers_lock:
            cls._point_triggers[name] = action

    @classmethod
    def clear_debug_triggers(cls) -> None:
        with cls._triggers_lock:
            cls._point_triggers.clear()

    @classmethod
    def release_debug_trigger_semaphore(cls, name: str) -> None:
        with cls._triggers_lock:
            action = cls._point_triggers.get(name)
        if action is not None and action.semaphore is not None:
            action.semaphore.release()
