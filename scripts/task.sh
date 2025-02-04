#!/usr/bin/env bash
coordinator_node=$1
HOSTNAME=$(hostname)
REMAINDER=$(($SLURM_PROCID % 5))

SIGNAL_DIR="../$SLURM_JOB_ID"
READY_FILE="$SIGNAL_DIR/${HOSTNAME}_ready"
DONE_FILE="$SIGNAL_DIR/done_$SLURM_PROCID"
EXIT_FILE="$SIGNAL_DIR/exit_$HOSTNAME"

OUTPUT_DIR="../output"

NUM_CLIENTS=20
CLIENT_TASKS=(1 2 3 4 6 7 8 9 11 12 13 14 16 17 18 19 21 22 23 24)

NODES=($(scontrol show hostnames $SLURM_NODELIST))

signal_db_ready() {
	touch $READY_FILE
}

wait_db_ready() {
	local all_ready=0
	while [ $all_ready -eq 0 ]; do
        local ready_count=0
        for node_name in "${NODES[@]}"; do
            if [ -f "$SIGNAL_DIR/${node_name}_ready" ]; then
                ready_count=$((ready_count + 1))
            fi
        done

        if [ $ready_count -eq 5 ]; then
            all_ready=1
        else
			sleep 1
		fi
    done
}

signal_client_done() {
	touch $DONE_FILE
}

wait_clients_done() {
    local all_done=0
    while [ $all_done -eq 0 ]; do
        local done_count=0
        for client_id in "${CLIENT_TASKS[@]}"; do
            if [ -f "$SIGNAL_DIR/done_$client_id" ]; then
                done_count=$((done_count + 1))
            fi
        done

        if [ $done_count -eq $NUM_CLIENTS ]; then
            all_done=1
        else
            sleep 1
        fi
    done
}

signal_worker_exit() {
	touch $EXIT_FILE
}

wait_workers_exit() {
	local all_exit=0
	while [ $all_exit -eq 0 ]; do
        local exit_count=0
        for node_name in "${NODES[@]}"; do
            if [ -f "$SIGNAL_DIR/exit_$node_name" ]; then
                exit_count=$((exit_count + 1))
            fi
        done

        if [ $exit_count -eq 5 ]; then
            all_exit=1
        else
			sleep 1 # Sleep to avoid busy waiting
		fi
    done
}

signal_output_done() {
    touch "$SIGNAL_DIR/OUTPUT"
}

wait_output_done() {
    while [ ! -f "$SIGNAL_DIR/OUTPUT" ]; do
        sleep 1
    done
}

signal_data_ready() {
    touch "$SIGNAL_DIR/DATA_READY"
}

wait_data_ready() {
    while [ ! -f "$SIGNAL_DIR/DATA_READY" ]; do
        sleep 1
    done
}

clean_up() {
	rm -rf $SIGNAL_DIR
    for i in $(seq 0 19); do
        rm "../output/${i}.csv"
    done
}

get_config_files() {
    POSTGRESQL_CONF="$PGDATA/postgresql.conf"
    PG_HBA_CONF="$PGDATA/pg_hba.conf"
    
    # Check if the configuration files exist
    if [ -f "$POSTGRESQL_CONF" ] && [ -f "$PG_HBA_CONF" ]; then
        # Define destination directory
        CONFIG_DIR="../config/"
        
        # Create the directory if it does not exist
        mkdir -p "$CONFIG_DIR"
        
        # Copy the files to the destination directory
        cp "$POSTGRESQL_CONF" "$CONFIG_DIR"
        cp "$PG_HBA_CONF" "$CONFIG_DIR"
        
        echo "Configuration files copied to $CONFIG_DIR"
    else
        echo "ERROR: One or both configuration files are missing!"
        return 1
    fi
}

if [ ${REMAINDER} -eq 0 ]; then
	mkdir -p $SIGNAL_DIR # prepare for file-based synchronization
    mkdir -p $OUTPUT_DIR # prepare output dir if it doesn't exist

	# Start db servers
	$HOME/pgsql/bin/pg_ctl -D $PGDATA -l logfile restart
    signal_db_ready

	if [ "${HOSTNAME}" = "$coordinator_node" ]; then
        wait_db_ready
		source "$HOME/${PGUSER}_venv/bin/activate"
		echo "Loading data"
        python ../python/table_creation.py $coordinator_node
        python ../python/data_ingestion.py $coordinator_node --w "../data_files/warehouse.csv" --d "../data_files/district.csv" --c "../data_files/customer.csv" --o "../data_files/order.csv" --i "../data_files/item.csv" --ol "../data_files/order-line.csv" --s "../data_files/stock.csv"
        signal_data_ready
    fi
fi

wait_data_ready

if [ ${REMAINDER} -ne 0 ]; then
	source "$HOME/${PGUSER}_venv/bin/activate"
	echo "Running driver program..."
    python ../python/driver.py $SLURM_PROCID $coordinator_node

	signal_client_done
fi

if [ ${REMAINDER} -eq 0 ]; then
	if [ "${HOSTNAME}" = "$coordinator_node" ]; then
        wait_clients_done
        python ../python/output_stats.py
        python ../python/end_state.py
        signal_output_done

        get_config_files
        
        signal_worker_exit
        wait_workers_exit

		echo "Performing clean up..."
		clean_up
    else
        wait_output_done
        signal_worker_exit
	fi
	# stop db servers
	$HOME/pgsql/bin/pg_ctl -D $PGDATA stop
fi
