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
    
else
    true
fi

sleep 5

# Steps to execute on coordinator node
if [ ${REMAINDER} -eq 0 ] && [ "${HOSTNAME}" = "$coordinator_node" ]; then
    psql -c "SELECT * FROM citus_set_coordinator_host('xcne0', $PGPORT);"
    psql -c "SELECT * FROM citus_add_node('xcne1', $PGPORT);"
    psql -c "SELECT * FROM citus_add_node('xcne2', $PGPORT);"
    psql -c "SELECT * FROM citus_add_node('xcne3', $PGPORT);"
    psql -c "SELECT * FROM citus_add_node('xcne4', $PGPORT);"

    psql -c "SELECT * FROM citus_get_active_worker_nodes();"
else
    true
fi

sleep 10

