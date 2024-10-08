#!/usr/bin/env bash
#SBATCH --job-name=citus
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=5
#SBATCH --partition=normal
#SBATCH --nodelist=xcne[0-4]
#SBATCH --time=120

coordinator_node=$(scontrol show hostnames | head -n 1)
echo $coordinator_node

# setup citus multi-node cluster
srun -l bash start_citus.sh $coordinator_node
wait
# check to see if citus settings perist
srun -l bash test_citus.sh $coordinator_node
wait
srun -l bash slurm_task.sh $coordinator_node
wait
