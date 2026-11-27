"""
core/fault_tolerance.py
إدارة إعادة جدولة المهام ومحاكاة/رصد أعطال الـ workers في التنفيذ الموزع
Task retry/rescheduling and worker failure simulation/detection for distributed runs.
"""

import time
import random
from typing import Any, Callable, Dict, List, Optional


class TaskRecoveryManager:
    def __init__(self, max_retries: int = 2, simulate_failure: bool = False, seed: int = 42):
        self.max_retries = max_retries
        self.simulate_failure = simulate_failure
        self.rng = random.Random(seed)
        self.completed_tasks: int = 0
        self.failed_tasks: int = 0
        self.recovered_tasks: int = 0
        self.event_logs: List[str] = []

    def _log(self, message: str):
        self.event_logs.append(message)

    def run_with_recovery(
        self,
        tasks: List[Any],
        worker_fn: Callable[[Any], Any],
        dask_client: Optional[Any] = None,
        fail_once_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        ينفذ قائمة مهام مع رصد الفشل وإعادة جدولة المهام غير المكتملة.
        يعمل مع Dask Distributed أو دالة محلية.
        """
        if fail_once_index is None and len(tasks) > 0:
            fail_once_index = len(tasks) // 2

        results: List[Any] = []
        task_times: List[float] = []

        for idx, task_payload in enumerate(tasks):
            attempts = 0
            succeeded = False

            while attempts <= self.max_retries and not succeeded:
                t0 = time.perf_counter()
                try:
                    # محاكاة فشل worker مرة واحدة على مهمة محددة عند تفعيل الخيار
                    if self.simulate_failure and idx == fail_once_index and attempts == 0:
                        raise RuntimeError(f"Simulated worker failure on task #{idx}")

                    if dask_client is not None:
                        future = dask_client.submit(worker_fn, task_payload)
                        out = future.result()
                    else:
                        out = worker_fn(task_payload)

                    dt = round(time.perf_counter() - t0, 5)
                    task_times.append(dt)
                    results.append(out)
                    self.completed_tasks += 1
                    succeeded = True

                    if attempts > 0:
                        self.recovered_tasks += 1
                        self._log(f"Task #{idx} recovered on attempt {attempts + 1}.")

                except Exception as exc:
                    attempts += 1
                    self.failed_tasks += 1
                    self._log(f"Task #{idx} failed (attempt {attempts}): {exc}")

                    if attempts <= self.max_retries:
                        self._log(f"Rescheduling task #{idx} (retry #{attempts})...")
                        time.sleep(0.05)
                    else:
                        self._log(f"Task #{idx} exceeded max retries and was marked failed.")

        return {
            "results": results,
            "task_times": task_times,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "recovered_tasks": self.recovered_tasks,
            "logs": self.event_logs,
        }