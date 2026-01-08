
# server/job_manager.py
import threading
import numpy as np
import json
import pandas as pd
from scipy.interpolate import interp1d
import io
import time
import os

class JobManager:
    def __init__(self, server_socket=None):
        self.jobs = []
        self.subscribed_power = self._load_subscribed_power()
        self.lock = threading.Lock()
        self._job_counter = 0
        self.server_socket = server_socket  # Reference to ServerSocket
        self.current_clearing_price = None
        self.negotiating = False
        self.market_clearing_execution_time = None

    def add_job(self, job_name, initial_utilization, perf_data):
        with self.lock:
            df = pd.read_json(io.StringIO(perf_data)).reset_index(drop=True)
            interp_func = self._create_interp_func(perf_data)
            job_id = 'job_id_' + str(self._job_counter)
            max_reduction = df["Resource Reduction"].max()
            power_max = df["Power"].max()

            self.jobs.append({
                "job_id": job_id,
                "job_name": job_name,
                "perf_data": perf_data,
                "power_func": interp_func,
                "max_reduction": max_reduction,
                "initial_utilization": initial_utilization,
                "current_bid": None,
                "power_max": power_max,
            })
            print(f"[JobManager] Job added: {job_id}")
            self._job_counter += 1

        total_power = self.get_total_power()
        print(f"[JobManager] Total power after job: {total_power:.2f} (Limit: {self.subscribed_power})")

        # Trigger MPR-INT if over power budget
        if total_power > self.subscribed_power and self.server_socket:
            if not self.negotiating:
                self.negotiating = True
                print("[JobManager] Subscribed power exceeded. Scheduling MPR-INT negotiation in 20s...")
                threading.Thread(target=self._delayed_negotiate, daemon=True).start()
            else:
                print("[JobManager] Negotiation already scheduled/in-progress; skipping trigger.")

        return job_id
    
    def get_total_required_power(self):
        total_power = 0
        for job in self.jobs:
            df = pd.read_json(io.StringIO(job["perf_data"])) 
            df = df.reset_index(drop=True)
            max_required_power = df["Power"].max()
            total_power += max_required_power
        return total_power
    
    def negotiate_and_finalize(self, C_target):
        start = time.perf_counter()
        supply_array = self.server_socket.initiate_mpr_int_negotiation(C_target)
        self.mark_negotiation_complete(supply_array)
        elapsed = time.perf_counter() - start
        self.market_clearing_execution_time = elapsed
        print(f"[Time-log] x and finalization completed in {elapsed:.2f} seconds.")

    def _delayed_negotiate(self):
        """Delay negotiation start to allow jobs to settle; ensures flag resets on failure.

        Recompute C_target at start to reflect the latest workload."""
        try:
            time.sleep(20)
            C_target = self.get_total_required_power() - (self.subscribed_power - 0.5)
            self.negotiate_and_finalize(C_target)
        except Exception as e:
            print(f"[JobManager] Negotiation failed: {e}")
            self.negotiating = False

    def mark_negotiation_complete(self,supplyValues):
        self.negotiating = False
        supply_list = supplyValues[0]  # Unpack job-level supply entries
        # print("[JobManager] Negotiation complete. Supply values:", supply_list)

        with self.lock:
            job_map = {entry["job_id"]: entry for entry in supply_list}
            for i,job in enumerate(self.jobs):
                job_id = job["job_id"]
                if job_id in job_map:
                    if i == 0:
                        self.current_clearing_price = float(job_map[job_id]["clearing_price_q"])
                        print(f"[JobManager] Clearing price set to: {self.current_clearing_price:.4f}")
                    delta_m = float(job_map[job_id]["delta_m"])
                    current_bid = float(job_map[job_id]["bid"])
                    new_utilization = 1.0 - delta_m
                    job["initial_utilization"] = new_utilization
                    job["current_bid"] = current_bid

                    power_func = job["power_func"]
                    new_power = float(power_func(delta_m))
                    job["current_power"] = new_power

                    # print(f"[JobManager] {job_id}: delta_m={delta_m:.4f}, utilization={new_utilization:.4f}, power={new_power:.2f}")

            total_power = self.get_total_power()
            print(f"[JobManager] Updated total power after negotiation: {total_power:.2f}")
        
    def _create_interp_func(self, perf_data):
        # ← convert string to DataFrame
        df = pd.read_json(io.StringIO(perf_data)) 
        df = df.reset_index(drop=True)

        resources = df["Resource Reduction"].to_numpy()
        powers = df["Power"].to_numpy()

        return interp1d(resources, powers, bounds_error=False, fill_value="extrapolate")

    def get_total_power(self):
        total_power = 0
        for job in self.jobs:
            job_id = job["job_id"]
            resource = job["initial_utilization"]  # Use initial utilization for power calculation
            resource_reduction = 1 - resource
            if "power_func" in job:
                total_power += float(job["power_func"](resource_reduction))
        return total_power

    def _load_subscribed_power(self):
        raw = os.getenv("SUBSCRIBED_POWER", "1000")
        try:
            return float(raw)
        except ValueError:
            print("[JobManager] Invalid SUBSCRIBED_POWER env value. Falling back to 1000.0")
            return 1000.0
