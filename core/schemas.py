from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime


@dataclass
class ExperimentConfig:
    algorithm: str
    mode: str
    dataset_size: int = 100_000
    extra_param: int = 5
    num_workers: int = 4
    chunk_size: int = 10_000
    repetitions: int = 1
    random_seed: int = 42
    scheduling_type: str = "Dynamic"
    enable_load_balancing: bool = True
    simulate_worker_failure: bool = False
    dataset_path: Optional[str] = None


@dataclass
class ExperimentResult:
    experiment_id: str
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    algorithm: str = ""
    mode: str = ""
    dataset_size: int = 0
    num_workers: int = 1
    chunk_size: int = 0

    execution_time: float = 0.0
    cpu_usage_avg: float = 0.0
    memory_peak_mb: float = 0.0
    completed_tasks: int = 0
    failed_tasks: int = 0
    task_execution_times: List[float] = field(default_factory=list)
    worker_times: Dict[str, float] = field(default_factory=dict)
    communication_overhead: float = 0.0

    speedup: float = 1.0
    efficiency: float = 1.0

    algorithm_output: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)
    status: str = "Completed"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)