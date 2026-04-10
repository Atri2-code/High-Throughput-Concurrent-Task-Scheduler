# Distributed DAG Job Scheduler

A high-throughput, concurrent execution engine built in Python. This system schedules and executes interdependent computational jobs using Directed Acyclic Graphs (DAGs) to resolve dependencies and prevent deadlocks.

## Architecture
1. **DAG Resolver**: Uses Kahn's Algorithm to topologically sort incoming tasks and group them into concurrently executable layers. Includes cycle detection to prevent deadlocks.
2. **Execution Engine**: Utilizes Python's `ProcessPoolExecutor` to dispatch jobs to parallel worker nodes, maximizing CPU utilization for heavy computational pipelines.

## Execution Flow
The scheduler parses a JSON configuration of tasks. Jobs without dependencies are instantly dispatched to the worker pool. Dependent jobs remain blocked until their parent tasks emit a success signal. 

## Quick Start
```bash
# Clone the repository
git clone [https://github.com/Atri2-code/Distributed-DAG-Scheduler.git](https://github.com/Atri2-code/Distributed-DAG-Scheduler.git)
cd Distributed-DAG-Scheduler

# Execute the test pipeline
python scheduler.py
