# scheduler.py
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dag_resolver import DAGResolver

def mock_worker_task(job_name, duration):
    """Simulates a heavy computational workload."""
    print(f"[WORKER] Starting job: {job_name} (Duration: {duration}s)")
    time.sleep(duration)
    print(f"[WORKER] Completed job: {job_name}")
    return job_name

class JobScheduler:
    def __init__(self, filepath, max_workers=4):
        self.filepath = filepath
        self.max_workers = max_workers
        self.jobs_data = self._load_jobs()
        self.resolver = DAGResolver()

    def _load_jobs(self):
        with open(self.filepath, 'r') as f:
            return json.load(f)

    def run(self):
        print("Initializing DAG Resolver...")
        self.resolver.build_from_jobs(self.jobs_data)
        
        try:
            layers = self.resolver.get_execution_layers()
        except ValueError as e:
            print(f"SCHEDULER ERROR: {e}")
            return

        print(f"Job Graph resolved into {len(layers)} execution layers.\n")

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            for layer_index, layer in enumerate(layers):
                print(f"--- Executing Layer {layer_index + 1}: {layer} ---")
                
                # Submit jobs in the current layer to the process pool
                futures = {
                    executor.submit(
                        mock_worker_task, 
                        job, 
                        self.jobs_data[job].get('duration', 1)
                    ): job for job in layer
                }

                # Wait for the current layer to finish before moving to the next
                for future in as_completed(futures):
                    finished_job = futures[future]
                    try:
                        result = future.result()
                        print(f"[SCHEDULER] Acknowledged completion of {result}")
                    except Exception as exc:
                        print(f"[SCHEDULER] Job {finished_job} generated an exception: {exc}")
                        
        print("\nAll pipelines executed successfully.")

if __name__ == "__main__":
    scheduler = JobScheduler("jobs.json", max_workers=3)
    scheduler.run()
