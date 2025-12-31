import numpy as np
import pandas as pd
import time
from main import maximize_net_gain_with_data_brute, maximize_net_gain_with_data_brute_vectorized
from functools import lru_cache


@lru_cache(maxsize=256)
def cached_bid_gain(q, delta_max, rr_tuple, ee_tuple):
    """Cache wrapper for the main optimizer; rr/ee must be hashable (tuples)."""
    rr = np.fromiter(rr_tuple, dtype=float)
    ee = np.fromiter(ee_tuple, dtype=float)
    bid, gain, _ = maximize_net_gain_with_data_brute(rr, ee, q, delta_max)
    return bid, gain


def run_tests():
    # Load a sample sheet
    DF_PATH = "Data/all_model_data.xlsx"
    SHEET = "minife"  # adjust if you want to test other jobs

    df = pd.read_excel(DF_PATH, sheet_name=SHEET)
    df_sorted = df.sort_values("Resource Reduction")
    rr = df_sorted["Resource Reduction"].to_numpy()
    ee = df_sorted["Extra Execution"].to_numpy()
    delta_max = df["Resource Reduction"].max()

    print(f"Testing maximize_net_gain_with_data_brute for sheet '{SHEET}'\n")
    for q in np.linspace(0.5, 5, 50):
        t0 = time.perf_counter()
        bid, gain = cached_bid_gain(q, delta_max, tuple(rr), tuple(ee))
        t1 = time.perf_counter()
        bid_vec, gain_vec, _ = maximize_net_gain_with_data_brute_vectorized(rr, ee, q, delta_max)
        t2 = time.perf_counter()
        print(
            f"q={q:.2f} -> bid={bid:.4f} (vec {bid_vec:.4f}), "
            f"gain={gain:.4f} (vec {gain_vec:.4f}), "
            f"time brute force : {1000*(t1-t0):.3f} ms vs time vector {1000*(t2-t1):.3f} ms"
        )

    q_example = 1.5
    t0 = time.perf_counter()
    bid, gain = cached_bid_gain(q_example, delta_max, tuple(rr), tuple(ee))
    t1 = time.perf_counter()
    bid_vec, gain_vec, _ = maximize_net_gain_with_data_brute_vectorized(rr, ee, q_example, delta_max)
    t2 = time.perf_counter()
    print(
        f"\nExample at q={q_example}: bid={bid:.4f} (vec {bid_vec:.4f}), gain={gain:.4f} (vec {gain_vec:.4f}), "
        f"time brute force : {1000*(t1-t0):.3f} ms vs time vector {1000*(t2-t1):.3f} ms"
    )


if __name__ == "__main__":
    run_tests()
