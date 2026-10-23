from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from ._enterprise import AutoUpgradePass
from ._logger import dbos_logger

if TYPE_CHECKING:
    from ._dbos import DBOS


class AutoUpgradeThread(threading.Thread):
    def __init__(
        self, dbos: DBOS, upgrade_pass: AutoUpgradePass, *, interval_sec: float
    ) -> None:
        super().__init__(daemon=True, name="dbos-auto-upgrade")
        self.dbos = dbos
        self.upgrade_pass = upgrade_pass
        self.interval_sec = interval_sec
        self.stop_event = threading.Event()

    def start(self) -> None:
        dbos_logger.debug(
            f"Starting auto-upgrade thread, one pass every {self.interval_sec} seconds"
        )
        super().start()

    def stop(self) -> None:
        self.stop_event.set()
        self.join(
            timeout=5.0
        )  # Give some grade period for whatever current transaction is running
        if self.is_alive():
            dbos_logger.warning("Auto-upgrade thread did not exit within timeout")
        try:
            self.upgrade_pass.close()
        except Exception as e:
            dbos_logger.warning(f"Auto-upgrade pass did not close cleanly: {e}")

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.upgrade_pass.run(self.stop_event)
            except Exception as e:
                dbos_logger.warning(f"Auto-upgrade pass failed: {e}", exc_info=e)
            if self.stop_event.wait(self.interval_sec):
                break
