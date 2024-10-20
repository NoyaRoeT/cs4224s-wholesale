#!/usr/bin/env bash
#SBATCH --job-name=citus
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=normal
#SBATCH --exclude=xcnc[0-6,8-15,17-28,30-32,34-35,37-39],xcne[0-5],xcnf[0-21],xcng[0-1]  # Exclude all other nodes
#SBATCH --time=180
#SBATCH --wait

if [ ! -x "$HOME/pgsql/bin/postgres" ]; then
    echo "Running install-citus.sh on $(hostname)..."
    bash ./citus/install-citus.sh
else
    echo "PostgreSQL is already installed on $(hostname)."
fi

VENV_NAME="${PGUSER}_venv"
VENV_DIR="$HOME/$VENV_NAME"

if [ ! -d "$VENV_DIR" ]; then
    python -m venv "$HOME/$VENV_NAME"
    echo "Virtual environment '$VENV_NAME' created successfully in the home directory."
else
    echo "Virtual environment '$VENV_NAME' already exists."
fi

source "$VENV_DIR/bin/activate"
pip install -r ../python/requirements.txt
deactivate