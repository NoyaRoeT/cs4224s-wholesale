import csv

NUM_CLIENTS = 20
OUTPUT_PATH = "../output"

def main():
    rows = []
    for i in range(0, NUM_CLIENTS):
        client_file_path = f"{OUTPUT_PATH}/{i}.csv"
        with open(client_file_path, mode='r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                rows.append(row)
    
    throughputs = [float(row[3]) for row in rows]
    
    max_throughput = round(max(throughputs), 2)
    min_throughput = round(min(throughputs), 2)
    avg_throughput = round(sum(throughputs) / len(throughputs), 2)

    clients_file_path = f'{OUTPUT_PATH}/clients.csv'
    sorted_rows = sorted(rows, key=lambda row: row[0])
    with open(clients_file_path, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(sorted_rows)  # Write the data


    throughput_file_path = f'{OUTPUT_PATH}/throughput.csv'
    with open(throughput_file_path, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows([[min_throughput, max_throughput, avg_throughput]])  # Write the data


if __name__ == "__main__":
    main()