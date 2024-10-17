import time

class ClientStat:
    def __init__(self):
        self.exec_times = []

    def record_xact(self, xact_func, *args):
        start_time = time.time()
        result = xact_func(*args)

        exec_time = time.time() - start_time
        self.exec_times.append(exec_time)
        return result
    
    def get_num_xacts(self):
        return len(self.exec_times)
    
    def get_total_exec_time(self):
        return sum(self.exec_times)

    def get_throughput(self):
        total_time = self.get_total_exec_time()
        if total_time == 0:
            return None
        num_xacts = self.get_num_xacts()
        return num_xacts / total_time
    
    def get_avg_xact_latency(self):
        throughput = self.get_throughput()
        if throughput is None:
            return None
        return 1000 / throughput
    
    def get_median_xact_latency(self):
        sorted_times = sorted(self.exec_times)
        n = len(sorted_times)
        mid = n // 2

        if (n % 2 == 1):
            median = sorted_times[mid]
        else:
            median = (sorted_times[mid] + sorted_times[mid - 1]) / 2
        
        return median * 1000 
    
    def get_p95_xact_latency(self):
        sorted_times = sorted(self.exec_times)
        idx = int(len(sorted_times) * 0.95)
        
        return sorted_times[idx] * 1000
    
    def get_p99_xact_latency(self):
        sorted_times = sorted(self.exec_times)
        idx = int(len(sorted_times) * 0.99)
        
        return sorted_times[idx] * 1000