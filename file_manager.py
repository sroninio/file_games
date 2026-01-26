#!/usr/bin/env python3

import os
import subprocess
from queue import Queue
import threading
from threading import BoundedSemaphore, Lock
import random
import asyncio
import time
from abc import ABC, abstractmethod

class RateLimitter:
    def __init__(self, limit_bytes_per_second):
        self.limit_bytes_per_second = limit_bytes_per_second
        self.lock = Lock()
        self.curr_second = 0
        self.bytes_in_curr_second = 0
    
    def wait_for_allowance(self, bytes_to_allow):
        while True:
            current_timestamp = time.time()
            with self.lock:
                if self.curr_second < int(current_timestamp):
                    self.curr_second = int(current_timestamp)
                    self.bytes_in_curr_second = 0
                if self.bytes_in_curr_second + bytes_to_allow <= self.limit_bytes_per_second:
                    self.bytes_in_curr_second += bytes_to_allow
                    break            
            time_to_sleep = (int(current_timestamp) + 1) - current_timestamp
            time.sleep(time_to_sleep)
                

class EfficientRandomPopContainer:
    def __init__(self, max_elements):
        self.max_elements = max_elements
        self.elements = [None] * max_elements
        self.curr_elements = 0
    
    def add_element(self, element):
        self.elements[self.curr_elements] = element
        self.curr_elements += 1
    
    def pop_random_element(self):
        random_idx = random.randint(0, self.curr_elements - 1)
        element = self.elements[random_idx]
        self.elements[random_idx] = self.elements[self.curr_elements - 1]
        self.curr_elements -= 1
        return element

class BaseFileManager(ABC):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int):
        self.base_path = base_path
        self.num_files = num_files
        self.file_size = file_size
        self.num_workers = num_workers
        self.max_write_waiters = max_write_waiters
        self.rate_limiter = RateLimitter(rate_limit_bytes_per_second) if rate_limit_bytes_per_second > 0 else None
        self.write_semaphore = BoundedSemaphore(max_write_waiters)
        self.dummy_buf = bytearray(file_size)
        
        if os.path.exists(self.base_path):
            subprocess.run(f"rm -rf {self.base_path}", shell=True, check=True)
        os.makedirs(self.base_path)
    
    @abstractmethod
    def write_kv_single_file(self, worker_id, to_delete):
        pass
    
    @abstractmethod
    def read_kv_single_file(self, worker_id):
        pass
    
    def async_write_kv(self):
        loop = asyncio.get_event_loop()
        return asyncio.gather(*(loop.run_in_executor(None, self.write_kv_single_file, i, True) for i in range(self.num_workers)))

    def async_read_kv(self):
        loop = asyncio.get_event_loop()
        return asyncio.gather(*(loop.run_in_executor(None, self.read_kv_single_file, i) for i in range(self.num_workers)))
    
    def sync_wait_for_place_in_write_queue(self):
        self.write_semaphore.acquire()
        self.write_semaphore.release()

class KVC2(BaseFileManager):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int):
        super().__init__(base_path, num_files, file_size, num_workers, max_write_waiters, rate_limit_bytes_per_second)
        kvc2_file_path = os.path.join(self.base_path, "kvc2")
        
        with open(kvc2_file_path, 'wb+') as f:
            for _ in range(num_files):
                f.write(self.dummy_buf)
            f.flush()      
        self.fds = [open(kvc2_file_path, 'rb+') for _ in range(num_workers)]
    
    def _seek_to_random_block(self, worker_id):
        random_block = random.randint(0, self.num_files - 1)
        offset = random_block * self.file_size
        self.fds[worker_id].seek(offset)

    def write_kv_single_file(self, worker_id, to_delete):
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size)
        self._seek_to_random_block(worker_id)
        self.fds[worker_id].write(self.dummy_buf)
    
    def read_kv_single_file(self, worker_id):
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size)
        self._seek_to_random_block(worker_id)
        dummy_read_buf = self.fds[worker_id].read(self.file_size)

class FileManager(BaseFileManager):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int):
        super().__init__(base_path, num_files, file_size, num_workers, max_write_waiters, rate_limit_bytes_per_second)
        self.files = EfficientRandomPopContainer(num_files)
        self.files_lock = Lock()  
        self.next_id = 0
        
        for _ in range(num_files):
            self.write_kv_single_file(0, False)
    
    def write_kv_single_file(self, worker_id, to_delete):
        self.write_semaphore.acquire()
        if to_delete:
            file_name_to_delete = self.pop_random_file()
            os.remove(file_name_to_delete) 
        file_name_to_create = self.create_file_name()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size)
        with open(file_name_to_create, 'wb') as f:
            f.write(self.dummy_buf)
        self.add_file(file_name_to_create)
        self.write_semaphore.release()
    
    def create_file_name(self):
        with self.files_lock:
            file_name_to_create = os.path.join(self.base_path, f"f{self.next_id}")
            self.next_id += 1 
        return file_name_to_create

    def read_kv_single_file(self, worker_id):
        file_name = self.pop_random_file()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size)
        with open(file_name, 'rb') as f:
            dummy_read_buf = f.read(self.file_size)
        self.add_file(file_name)

    def pop_random_file(self):
        with self.files_lock:
            file_name = self.files.pop_random_element()
        return file_name
    
    def add_file(self, file_name):
        with self.files_lock:
            self.files.add_element(file_name)

class System:
    def __init__(self, max_inflight_requests, max_write_waiters, num_workers_per_single_request, kv_base_path, num_files, file_size, requests_to_complete, rate_limit_bytes_per_second):
        self.max_inflight_requests = max_inflight_requests
        self.completed_requests = 0
        self.requests_to_complete = requests_to_complete
        self.num_workers_per_single_request = num_workers_per_single_request
        self.file_manager = FileManager(kv_base_path, num_files, file_size, num_workers_per_single_request, max_write_waiters, rate_limit_bytes_per_second)

            
    async def execute_single_request(self):
        return self.file_manager.async_read_kv()

    async def run_benchmark(self):
        start_time = time.time()
        last_print_time = start_time
        last_print_count = 0
        
        pending_requests = set()
        while self.completed_requests < self.requests_to_complete:
            while len(pending_requests) < self.max_inflight_requests and self.completed_requests + len(pending_requests) < self.requests_to_complete:
                self.file_manager.sync_wait_for_place_in_write_queue()
                pending_requests.add(self.execute_single_request()) 
            done, pending = await asyncio.wait(pending_requests, return_when=asyncio.FIRST_COMPLETED)
            self.completed_requests += len(done)
            pending_requests = pending 
            for _ in range(len(done)):
                asyncio.create_task(self.file_manager.async_write_kv())
            await asyncio.sleep(0)
            
            if self.completed_requests - last_print_count >= 1000:
                current_time = time.time()
                total_bw = self.completed_requests / (current_time - start_time)
                recent_bw = (self.completed_requests - last_print_count) / (current_time - last_print_time)
                print(f"Completed {self.completed_requests} requests | Total BW: {total_bw:.2f} req/s | Recent BW: {recent_bw:.2f} req/s")
                last_print_time = current_time
                last_print_count = self.completed_requests
        
        end_time = time.time()
        total_time = end_time - start_time
        overall_bw = self.completed_requests / total_time
        print(f"\nBenchmark completed!")
        print(f"Total requests: {self.completed_requests}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Overall BW: {overall_bw:.2f} req/s")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Run file system benchmark')
    parser.add_argument('max_inflight_requests', type=int, help='Maximum number of inflight requests')
    parser.add_argument('max_write_waiters', type=int, help='Maximum number of write waiters')
    parser.add_argument('num_workers_per_single_request', type=int, help='Number of workers per single request')
    parser.add_argument('kv_base_path', type=str, help='Base path for KV files')
    parser.add_argument('num_files', type=int, help='Number of files to create')
    parser.add_argument('file_size', type=int, help='Size of each file in bytes')
    parser.add_argument('requests_to_complete', type=int, help='Number of requests to complete')
    parser.add_argument('rate_limit_bytes_per_second', type=int, help='Rate limit in bytes per second (0 = no limit)')
    
    args = parser.parse_args()
    
    system = System(
        args.max_inflight_requests,
        args.max_write_waiters,
        args.num_workers_per_single_request,
        args.kv_base_path,
        args.num_files,
        args.file_size,
        args.requests_to_complete,
        args.rate_limit_bytes_per_second
    )
    
    asyncio.run(system.run_benchmark())

if __name__ == '__main__':
    main()
 