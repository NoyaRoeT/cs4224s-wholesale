#!/usr/bin/env bash
coordinator_node=$1
HOSTNAME=$(hostname)
REMAINDER=$(($SLURM_PROCID % 5))

# Steps to execute on all nodes
if [ ${REMAINDER} -eq 0 ]; then
	bash ./citus/init-citus-db.sh
    $HOME/pgsql/bin/pg_ctl -D $PGDATA -l logfile start
    createdb
    psql -p $PGPORT -c "CREATE EXTENSION citus;"
fi

sleep 10

# Steps to execute on coordinator node
if [ ${REMAINDER} -eq 0 ] && [ "${HOSTNAME}" = "$coordinator_node" ]; then
    psql -c "SELECT * FROM citus_set_coordinator_host('$coordinator_node', $PGPORT);"

    allocated_nodes=($(scontrol show hostnames $SLURM_NODELIST))

    for node in "${allocated_nodes[@]}"; do
        if [ "$node" != "$coordinator_node" ]; then
            psql -c "SELECT * FROM citus_add_node('$node', $PGPORT);"
        fi
    done
fi

sleep 10

