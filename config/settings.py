import os

DEFAULT_WORKERS = max(1, (os.cpu_count() or 4) - 1)
AVAILABLE_MODES = ["Sequential", "Multi-Core", "Distributed"]
AVAILABLE_ALGORITHMS = [
    "K-Means Clustering",
    "Graph Processing",
    "Monte Carlo Simulation",
]

CHUNK_SIZES = [100, 1000, 10000, 50000]
DEFAULT_CHUNK_SIZE = 10000

KMEANS_SIZES = [10_000, 100_000, 1_000_000]
MONTE_CARLO_SIZES = [100_000, 1_000_000, 10_000_000]

DASK_MEMORY_LIMIT = "2GB"
DEFAULT_RANDOM_SEED = 42
HISTORY_FILE = os.path.join("data", "experiments_history.json")