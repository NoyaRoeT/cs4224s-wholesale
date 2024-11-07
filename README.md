# Project Setup
## Moving files to xlogin
The first step is to ssh into an xlogin node by running:
```
ssh <username>@xlogin.comp.nus.edu.sg
```

Then copy the code directory to your home directory.

## Installation and Environemnt Setup
First, use `cd` to enter the scripts directory.

Then, use the `setup.sh` bash script to setup the project by running:
```
bash  setup.sh
```
This script will install PostgreSQL, Citus, a Python Virtual Environment and any required python packages in your home directory.

## Running the benchmarking task
First, make sure you are in the scripts directory, as all file paths assume that is your working directory.

Our install scripts are hardcoded to set PGUSER=cs4224s and PGPORT=5115.

In order to run the benchmarking experiment, PGUSER should be set to the username of our logged in account. To do this:
```
export PGUSER=<username>
```

Then, you can submit `submit_job.sh` as a slurm job by running:
```
sbatch submit_job.sh
```

At the end of the job, it will generate and output and config directory containing the results and configuration respectively.