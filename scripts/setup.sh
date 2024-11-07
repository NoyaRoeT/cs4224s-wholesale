#!/usr/bin/env bash
srun --job-name=citus \
     --nodes=1 \
     --ntasks-per-node=1 \
     --partition=normal \
     --exclude=xcnc[0-6,8-15,17-28,30-32,34-35,37-39],xcne[0-5],xcnf[0-21],xcng[0-1] \
     --time=180 \
     bash install.sh