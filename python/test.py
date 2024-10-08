import psycopg2
import sys

def main():
    hostname = sys.argv[1]
    pid = sys.argv[2]
    print(f'Hello world from {pid} on {hostname} ')


if __name__ == "__main__":
    main()