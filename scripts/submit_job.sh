#!/usr/bin/env bash
#SBATCH --job-name=citus
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=5
#SBATCH --partition=normal
#SBATCH --nodelist=xcnd*
#SBATCH --time=120

coordinator_node=$(scontrol show hostnames | head -n 1)
echo $coordinator_node

# setup citus multi-node cluster
srun -l bash ./citus/setup-cluster.sh $coordinator_node
wait

# run application
srun -l bash task.sh $coordinator_node
wait
