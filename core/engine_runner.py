"""
core/engine_runner.py
محرك التنفيذ الموحد للأوضاع الثلاثة: Sequential / Multi-Core / Distributed (Dask)
مع دعم مبدئي لجدولة المهام وتحمل الأعطال.
"""

import time
import uuid
import multiprocessing as mp
import importlib
from typing import Any, Callable, Dict, Optional

try:
    psutil = importlib.import_module("psutil")
except ImportError:
    psutil = None

try:
    dask_distributed = importlib.import_module("dask.distributed")
    Client = dask_distributed.Client
    LocalCluster = dask_distributed.LocalCluster
except ImportError:
    Client = None
    LocalCluster = None

from config.settings import DASK_MEMORY_LIMIT
from core.schemas import ExperimentConfig, ExperimentResult
from core.fault_tolerance import TaskRecoveryManager


class ComputationalEngine:
    def __init__(self):
        self._registry: Dict[str, Callable[[ExperimentConfig, Optional[Any], Optional[TaskRecoveryManager]], ExperimentResult]] = {}
        self._baseline_times: Dict[str, float] = {}

    def register_algorithm(self, name: str, handler_fn: Callable):
        """تسجيل خوارزمية جديدة في المحرك"""
        self._registry[name] = handler_fn

    def _create_dask_client(self, num_workers: int):
        """إنشاء Dask LocalCluster بإعدادات آمنة للذاكرة"""
        if Client is None or LocalCluster is None:
            raise RuntimeError("Dask Distributed is not installed. Run: pip install dask distributed")

        cluster = LocalCluster(
            n_workers=num_workers,
            threads_per_worker=1,
            memory_limit=DASK_MEMORY_LIMIT,
            silence_logs=40,
        )
        client = Client(cluster)
        return client, cluster

    def run_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """نقطة الدخول الموحدة لتشغيل أي تجربة"""
        if config.algorithm not in self._registry:
            raise ValueError(f"Algorithm '{config.algorithm}' is not registered yet.")

        handler = self._registry[config.algorithm]
        run_id = f"EXP-{uuid.uuid4().hex[:6].upper()}"

        recovery_mgr = TaskRecoveryManager(
            max_retries=2,
            simulate_failure=config.simulate_worker_failure,
            seed=config.random_seed,
        )

        process = psutil.Process() if psutil else None
        mem_before = process.memory_info().rss / (1024 * 1024) if process else 0.0

        client, cluster = None, None
        try:
            if config.mode == "Distributed":
                client, cluster = self._create_dask_client(config.num_workers)

            start_t = time.perf_counter()
            result: ExperimentResult = handler(config, client, recovery_mgr)
            elapsed = time.perf_counter() - start_t

            result.experiment_id = run_id
            result.algorithm = config.algorithm
            result.mode = config.mode
            result.dataset_size = config.dataset_size
            result.num_workers = 1 if config.mode == "Sequential" else config.num_workers
            result.chunk_size = config.chunk_size

            if result.execution_time <= 0:
                result.execution_time = round(elapsed, 4)

            # دمج إحصائيات الاستعادة لو تم استخدام recovery_mgr
            if recovery_mgr.completed_tasks > 0 or recovery_mgr.failed_tasks > 0:
                result.completed_tasks = recovery_mgr.completed_tasks
                result.failed_tasks = recovery_mgr.failed_tasks
                result.logs.extend(recovery_mgr.event_logs)

            result.logs.insert(0, f"Scheduling mode: {config.scheduling_type} | Load balancing: {config.enable_load_balancing}")

            if process:
                mem_after = process.memory_info().rss / (1024 * 1024)
                result.memory_peak_mb = round(max(mem_after, mem_before), 2)
                result.cpu_usage_avg = psutil.cpu_percent(interval=None)

            base_key = f"{config.algorithm}_{config.dataset_size}_{config.extra_param}"
            if config.mode == "Sequential":
                self._baseline_times[base_key] = result.execution_time
                result.speedup = 1.0
                result.efficiency = 1.0
            elif base_key in self._baseline_times and result.execution_time > 0:
                t_seq = self._baseline_times[base_key]
                result.speedup = round(t_seq / result.execution_time, 3)
                result.efficiency = round(result.speedup / max(1, result.num_workers), 3)

            return result

        finally:
            if client is not None:
                client.close()
            if cluster is not None:
                cluster.close()


def _work_unit(x: int) -> int:
    """دالة مستقلة على مستوى الملف حتى يسهل عمل pickle لها في Windows multiprocessing"""
    return sum(i * i for i in range(x))


def _dummy_algo_handler(
    config: ExperimentConfig,
    dask_client: Optional[Any] = None,
    recovery_mgr: Optional[TaskRecoveryManager] = None,
) -> ExperimentResult:
    """دالة تجريبية للتأكد إن الـ Engine والـ Fault Tolerance شغالين صح"""
    chunks = [config.chunk_size] * max(1, config.dataset_size // config.chunk_size)
    t0 = time.perf_counter()

    if config.mode == "Distributed" and recovery_mgr is not None:
        rec = recovery_mgr.run_with_recovery(chunks, _work_unit, dask_client=dask_client)
        outputs = rec["results"]
        completed = rec["completed_tasks"]
        failed = rec["failed_tasks"]
        task_times = rec["task_times"]
    elif config.mode == "Sequential":
        t_start = time.perf_counter()
        outputs = []
        task_times = []
        for c in chunks:
            ct0 = time.perf_counter()
            outputs.append(_work_unit(c))
            task_times.append(round(time.perf_counter() - ct0, 5))
        completed = len(outputs)
        failed = 0
    elif config.mode == "Multi-Core":
        with mp.Pool(processes=config.num_workers) as pool:
            outputs = pool.map(_work_unit, chunks)
        completed = len(outputs)
        failed = 0
        task_times = []
    else:
        raise ValueError("Unsupported mode")

    exec_t = round(time.perf_counter() - t0, 4)
    return ExperimentResult(
        experiment_id="",
        execution_time=exec_t,
        completed_tasks=completed,
        failed_tasks=failed,
        task_execution_times=task_times,
        algorithm_output={"total_chunks": len(outputs)},
        logs=[f"Finished {completed} chunks using {config.mode}"],
    )


if __name__ == "__main__":
    mp.freeze_support()
    engine = ComputationalEngine()
    engine.register_algorithm("K-Means Clustering", _dummy_algo_handler)

    cfg_fail = ExperimentConfig(
        algorithm="K-Means Clustering",
        mode="Distributed",
        dataset_size=100_000,
        chunk_size=25_000,
        num_workers=2,
        simulate_worker_failure=True,
    )
    res_fail = engine.run_experiment(cfg_fail)
    print("Fault Tolerance Test -> Completed:", res_fail.completed_tasks, "| Failed:", res_fail.failed_tasks)
    for line in res_fail.logs:
        print("  LOG:", line)