import sys
import psycopg2
from transactions import get_xact_func
from client_stat import ClientStat

client_stat = ClientStat()

DB_HOST = 'xcne0'
DB_PORT = '5115'
DB_NAME = 'project'
DB_USER = 'cs4224s'

def main():
    # Get slurm process id to determine xact file to read
    if (len(sys.argv) < 2):
        return
    
    proc_id = int(sys.argv[1])
    file_idx = proc_id - (1 + int(proc_id / 5))
    file_path = f"../xact_files/{file_idx}.txt"
    
    # connect to db
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
    )
    cursor = connection.cursor()

    with open(file_path, 'r') as xact_file:
        while True:
            line = xact_file.readline().strip()
            if not line:
                break
            params = line.split(",")
            results = handle_xact(params, xact_file, cursor)
            print(results) # output xact results to stdout
    print_client_stats(client_stat)
    
def handle_xact(params, xact_file, cursor):
    xact_key = params[0]

    # new order xact
    if xact_key == 'N':
        m = int(params[4])
        items = []
        for _ in range(m):
            item_line = xact_file.readline().strip()
            item_params = item_line.split(",")
            items.append(item_params)
        new_order_func = get_xact_func('N')
        args = params[1:-1]
        args.append(items)
        args.append(cursor)
        return client_stat.record_xact(new_order_func, *args)
    
    # rest of xacts
    args = params[1: len(params)]
    args.append(cursor)
    xact_func = get_xact_func(xact_key)
    return client_stat.record_xact(xact_func, *args)

# output stats for this client to stderr
def print_client_stats(client_stat):
    print(f"Total number of transactions: {client_stat.get_num_xacts()}", file=sys.stderr)
    print(f"Total execution time (seconds): {client_stat.get_total_exec_time():.2f}", file=sys.stderr)
    print(f"Transaction throughput (transactions/second): {client_stat.get_throughput():.2f}", file=sys.stderr)
    print(f"Average transaction latency (ms): {client_stat.get_avg_xact_latency():.2f}", file=sys.stderr)
    print(f"Median transaction latency (ms): {client_stat.get_median_xact_latency():.2f}", file=sys.stderr)
    print(f"95th percentile transaction latency (ms): {client_stat.get_p95_xact_latency():.2f}", file=sys.stderr)
    print(f"99th percentile transaction latency (ms): {client_stat.get_p99_xact_latency():.2f}", file=sys.stderr)
        


if __name__ == "__main__":
    main()