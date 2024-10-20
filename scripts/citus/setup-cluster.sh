#!/usr/bin/env bash
coordinator_node=$1
HOSTNAME=$(hostname)
REMAINDER=$(($SLURM_PROCID % 5))
HOSTNAMES=()

# Steps to execute on all nodes
if [ ${REMAINDER} -eq 0 ]; then
	bash ./citus/init-citus-db.sh
    $HOME/pgsql/bin/pg_ctl -D $PGDATA -l logfile start
    createdb
    psql -p $PGPORT -c "CREATE EXTENSION citus;"
    HOSTNAMES+=("$HOSTNAME")
    
else
    true
fi

sleep 10

# Steps to execute on coordinator node
if [ ${REMAINDER} -eq 0 ] && [ "${HOSTNAME}" = "$coordinator_node" ]; then
    psql -c "SELECT * FROM citus_set_coordinator_host($coordinator_node, $PGPORT);"
    for HOSTNAME in "${HOSTNAMES[@]}"; do
        psql -c "SELECT * FROM citus_add_node('$HOSTNAME', $PGPORT);"
    done
else
    true
fi

sleep 5

