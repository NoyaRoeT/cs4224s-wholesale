#!/usr/bin/env bash
#SBATCH --job-name=citus
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=5
#SBATCH --partition=normal
#SBATCH --nodelist=xcne[0-4]
#SBATCH --time=120

pip install psycopg2
python python/table_creation.py
wait
