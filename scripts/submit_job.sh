#!/usr/bin/env bash
#SBATCH --job-name=citus
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=5
#SBATCH --partition=normal
#SBATCH --exclude=xcnc[0-6,8-15,17-28,30-32,34-35,37-39],xcne[0-5],xcnf[0-21],xcng[0-1]  # Exclude all other nodes
#SBATCH --time=120

coordinator_node=$(scontrol show hostnames | head -n 1)
echo $coordinator_node

# setup citus multi-node cluster
srun -l bash ./citus/setup-cluster.sh $coordinator_node
wait

# run application
srun -l bash task.sh $coordinator_node
wait
