import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

def plot_memory_usage(log_file_paths, titles):
    num_files = len(log_file_paths)
    plt.figure(figsize=(20, num_files * 5))  # Width, Height

    for i, log_file_path in enumerate(log_file_paths):
        with open(log_file_path, 'r') as file:
            log_lines = file.readlines()

        # Parse the log data
        timestamps, res_values, shr_values = [], [], []

        for line in log_lines:
            parts = line.split()
            timestamp = datetime.strptime(parts[0] + ' ' + parts[1], '%Y-%m-%d %H:%M:%S,%f')
            res = int(parts[3].split(' ')[0]) / 1024  # Convert KB to MB
            shr = int(parts[6].split(' ')[0]) / 1024  # Convert KB to MB

            timestamps.append(timestamp)
            res_values.append(res)
            shr_values.append(shr)

        # Plotting the data for each log file
        ax = plt.subplot(num_files, 1, i + 1)
        ax.plot(timestamps, res_values, label='RES (MB)', marker='o')
        ax.plot(timestamps, shr_values, label='SHR (MB)', marker='x')

        # Formatting each subplot
        ax.set_xlabel('Time')
        ax.set_ylabel('Memory Size (MB)')
        ax.set_title(titles[i])
        ax.legend()
        ax.grid(True)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig("memory.png")
    plt.show()



if __name__ == "__main__":
    # Usage example
    log_files = [
        '/home/mochix/tantivy_search_memory/cpp_pool1_no_cache/488635_memory.log',
        '/home/mochix/tantivy_search_memory/rust_pool1_no_cache/489732_memory.log',
        '/home/mochix/tantivy_search_memory/cpp_pool1_with_cache/491084_memory.log',
        '/home/mochix/tantivy_search_memory/rust_pool1_with_cache/493543_memory.log',

        '/home/mochix/tantivy_search_memory/cpp_pool4_no_cache_iter1/644811_memory.log',
        '/home/mochix/tantivy_search_memory/rust_pool4_no_cache_iter1/655751_memory.log',
        '/home/mochix/tantivy_search_memory/cpp_pool4_with_cache_iter1/674402_memory.log',
        '/home/mochix/tantivy_search_memory/rust_pool4_with_cache_iter1/728967_memory.log',
    ]

    titles = [
        'cpp_pool1_no_cache',
        'rust_pool1_no_cache',
        'cpp_pool1_with_cache',
        'rust_pool1_with_cache',

        'cpp_pool4_no_cache_iter1',
        'rust_pool4_no_cache_iter1',
        'cpp_pool4_with_cache_iter1',
        'rust_pool4_with_cache_iter1',
    ]

    plot_memory_usage(log_files, titles)
