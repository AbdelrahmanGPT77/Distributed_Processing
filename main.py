"""
main.py
نقطة تشغيل المشروع الرئيسية وربط المكونات الأساسية
Main entry point for DistCompute Studio.
"""

import multiprocessing as mp
from core.engine_runner import ComputationalEngine, _dummy_algo_handler


def build_engine() -> ComputationalEngine:
    """تهيئة المحرك وتسجيل الخوارزميات المتاحة"""
    engine = ComputationalEngine()

    try:
        from algorithms.kmeans_engine import run_kmeans
        engine.register_algorithm("K-Means Clustering", run_kmeans)
    except ImportError:
        engine.register_algorithm("K-Means Clustering", _dummy_algo_handler)

    try:
        from algorithms.graph_engine import run_graph
        engine.register_algorithm("Graph Processing", run_graph)
    except ImportError:
        engine.register_algorithm("Graph Processing", _dummy_algo_handler)

    try:
        from algorithms.monte_carlo_engine import run_monte_carlo
        engine.register_algorithm("Monte Carlo Simulation", run_monte_carlo)
    except ImportError:
        engine.register_algorithm("Monte Carlo Simulation", _dummy_algo_handler)

    return engine


if __name__ == "__main__":
    mp.freeze_support()
    app_engine = build_engine()
    print("Engine built successfully. Registered algorithms:", list(app_engine._registry.keys()))