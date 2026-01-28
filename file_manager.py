#!/usr/bin/env python3

import os
import shutil
from threading import BoundedSemaphore, Lock
from queue import Queue
import random
import asyncio
import time
from abc import ABC, abstractmethod

class RateLimiter:
    def __init__(self, limit_bytes_per_second):
        self.limit_bytes_per_second = limit_bytes_per_second
        self.lock = Lock()
        self.curr_second = 0
        self.bytes_in_curr_second = 0
    
    def wait_for_allowance(self, bytes_to_allow, is_read):
        while True:
            current_timestamp = time.time()
            with self.lock:
                if self.curr_second < int(current_timestamp):
                    self.curr_second = int(current_timestamp)
                    self.bytes_in_curr_second = 0
                
                if self.bytes_in_curr_second + bytes_to_allow <= self.limit_bytes_per_second:
                    self.bytes_in_curr_second += bytes_to_allow
                    break            
                else:
                    op_type = "READ" if is_read else "WRITE"
                    #print(f"[RateLimiter] {op_type} - Second: {self.curr_second}, Request size: {bytes_to_allow}, Bytes used: {self.bytes_in_curr_second}/{self.limit_bytes_per_second}", flush=True)

            time_to_sleep = (int(current_timestamp) + 1) - current_timestamp
            time.sleep(time_to_sleep)
                

class EfficientRandomPopContainer:
    def __init__(self, max_elements):
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
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int, recreate_dir: bool):
        self.base_path = base_path
        self.num_files = num_files
        self.file_size = file_size
        self.num_workers = num_workers
        self.max_write_waiters = max_write_waiters
        self.rate_limiter = RateLimiter(rate_limit_bytes_per_second) if rate_limit_bytes_per_second > 0 else None
        self.write_semaphore = BoundedSemaphore(max_write_waiters)
        self.dummy_buf = bytearray(file_size)
        if recreate_dir:
            if os.path.exists(self.base_path):
                shutil.rmtree(self.base_path)
            os.makedirs(self.base_path)
    
    @abstractmethod
    def write_kv_single_file(self, worker_id, to_delete):
        pass
    
    @abstractmethod
    def read_kv_single_file(self, worker_id):
        pass
    
    def async_write_kv(self):
        loop = asyncio.get_running_loop()
        for i in range(self.num_workers):
            loop.run_in_executor(None, self.write_kv_single_file, i, True)

    async def async_read_kv(self):
        loop = asyncio.get_running_loop()
        return await asyncio.gather(*(loop.run_in_executor(None, self.read_kv_single_file, i) for i in range(self.num_workers)))
    
    def sync_wait_for_place_in_write_queue(self):
        self.write_semaphore.acquire()
        self.write_semaphore.release()

class KVC2(BaseFileManager):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, max_inflight_requests: int, rate_limit_bytes_per_second: int, recreate_dir: bool):
        super().__init__(base_path, num_files, file_size, num_workers, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        kvc2_file_path = os.path.join(self.base_path, "kvc2")
        with open(kvc2_file_path, 'wb+') as f:
            for _ in range(num_files):
                f.write(self.dummy_buf)
            f.flush()
        num_fds = (max_inflight_requests + max_write_waiters) * num_workers
        self.fd_queue = Queue(maxsize=num_fds)
        for _ in range(num_fds):
            self.fd_queue.put(open(kvc2_file_path, 'rb+'))
    
    def _seek_to_random_block(self, fd):
        random_block = random.randint(0, self.num_files - 1)
        offset = random_block * self.file_size
        fd.seek(offset)

    def write_kv_single_file(self, worker_id, to_delete):
        self.write_semaphore.acquire()
        fd = self.fd_queue.get(block=False)
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=False)
        self._seek_to_random_block(fd)
        fd.write(self.dummy_buf)
        self.fd_queue.put(fd)
        self.write_semaphore.release()
    
    def read_kv_single_file(self, worker_id):
        fd = self.fd_queue.get(block=False)
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=True)
        self._seek_to_random_block(fd)
        dummy_read_buf = fd.read(self.file_size)
        self.fd_queue.put(fd)

class FileManager(BaseFileManager):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int, recreate_dir: bool):
        super().__init__(base_path, num_files, file_size, num_workers, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        self.files = EfficientRandomPopContainer(num_files)
        self.files_lock = Lock()
        self.next_id = 0
        if recreate_dir:
            for _ in range(num_files):
                self._create_initial_file()
        else:
            max_id = -1
            for filename in os.listdir(self.base_path):
                assert filename.startswith('f') and filename[1:].isdigit(), f"Invalid file format: {filename}"
                file_id = int(filename[1:])
                max_id = max(max_id, file_id)
                self.add_file(os.path.join(self.base_path, filename))
            self.next_id = max_id + 1
    
    def _create_initial_file(self):
        file_name_to_create = self.create_file_name()
        with open(file_name_to_create, 'wb') as f:
            f.write(self.dummy_buf)
        self.add_file(file_name_to_create)
    
    def write_kv_single_file(self, worker_id, to_delete):
        self.write_semaphore.acquire()
        if to_delete:
            file_name_to_delete = self.pop_random_file()
            os.remove(file_name_to_delete) 
        file_name_to_create = self.create_file_name()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=False)
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
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=True)
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

class FileManagerNoEviction(FileManager):
    def write_kv_single_file(self, worker_id, to_delete):
        self.write_semaphore.acquire()
        file_name = self.pop_random_file()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=False)
        with open(file_name, 'wb') as f:
            f.write(self.dummy_buf)
        self.add_file(file_name)
        self.write_semaphore.release()

class FileManagerNoEvictionNoOpen(FileManagerNoEviction):
    def __init__(self, base_path: str, num_files: int, file_size: int, num_workers: int, max_write_waiters: int, rate_limit_bytes_per_second: int, recreate_dir: bool):
        super().__init__(base_path, num_files, file_size, num_workers, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        self.fd_map = {}
        file_list = []
        while self.files.curr_elements > 0:
            file_name = self.files.pop_random_element()
            file_list.append(file_name)
            self.fd_map[file_name] = open(file_name, 'rb+')
        for file_name in file_list:
            self.files.add_element(file_name)
    
    def write_kv_single_file(self, worker_id, to_delete):
        self.write_semaphore.acquire()
        file_name = self.pop_random_file()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=False)
        fd = self.fd_map[file_name]
        fd.seek(0)
        fd.write(self.dummy_buf)
        self.add_file(file_name)
        self.write_semaphore.release()
    
    def read_kv_single_file(self, worker_id):
        file_name = self.pop_random_file()
        if self.rate_limiter:
            self.rate_limiter.wait_for_allowance(self.file_size, is_read=True)
        fd = self.fd_map[file_name]
        fd.seek(0)
        dummy_read_buf = fd.read(self.file_size)
        self.add_file(file_name)

class System:
    def __init__(self, max_inflight_requests, max_write_waiters, num_workers_per_single_request, kv_base_path, num_files, file_size, requests_to_complete, rate_limit_bytes_per_second, file_manager_type, recreate_dir):
        self.max_inflight_requests = max_inflight_requests
        self.completed_requests = 0
        self.requests_to_complete = requests_to_complete
        self.num_workers_per_single_request = num_workers_per_single_request
        
        if file_manager_type == 'kvc2':
            self.file_manager = KVC2(kv_base_path, num_files, file_size, num_workers_per_single_request, max_write_waiters, max_inflight_requests, rate_limit_bytes_per_second, recreate_dir)
        elif file_manager_type == 'filemanager':
            self.file_manager = FileManager(kv_base_path, num_files, file_size, num_workers_per_single_request, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        elif file_manager_type == 'filemanagernoeviction':
            self.file_manager = FileManagerNoEviction(kv_base_path, num_files, file_size, num_workers_per_single_request, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        elif file_manager_type == 'filemanagernoevictionnoopen':
            self.file_manager = FileManagerNoEvictionNoOpen(kv_base_path, num_files, file_size, num_workers_per_single_request, max_write_waiters, rate_limit_bytes_per_second, recreate_dir)
        else:
            raise ValueError(f"Unknown file_manager_type: {file_manager_type}. Must be 'kvc2', 'filemanager', 'filemanagernoeviction', or 'filemanagernoevictionnoopen'")

            
    def execute_single_request(self):
        return asyncio.create_task(self.file_manager.async_read_kv())

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
                self.file_manager.async_write_kv()
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
        semaphore_value = self.file_manager.write_semaphore._value
        print(f"\n{'='*60}", flush=True)
        print(f"Benchmark completed!", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Total requests: {self.completed_requests}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Overall BW: {overall_bw:.2f} req/s")
        print(f"Final semaphore value: {semaphore_value}")
        


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Run file system benchmark')
    parser.add_argument('--max_inflight_requests', type=int, required=True, help='Maximum number of inflight requests')
    parser.add_argument('--max_write_waiters', type=int, required=True, help='Maximum number of write waiters')
    parser.add_argument('--num_workers_per_single_request', type=int, required=True, help='Number of workers per single request')
    parser.add_argument('--kv_base_path', type=str, required=True, help='Base path for KV files')
    parser.add_argument('--num_files', type=int, required=True, help='Number of files to create')
    parser.add_argument('--file_size', type=int, required=True, help='Size of each file in bytes')
    parser.add_argument('--requests_to_complete', type=int, required=True, help='Number of requests to complete')
    parser.add_argument('--rate_limit_bytes_per_second', type=int, required=True, help='Rate limit in bytes per second (0 = no limit)')
    parser.add_argument('--file_manager_type', type=str, required=True, choices=['kvc2', 'filemanager', 'filemanagernoeviction', 'filemanagernoevictionnoopen'], help='Type of file manager: kvc2, filemanager, filemanagernoeviction, or filemanagernoevictionnoopen')
    parser.add_argument('--recreate_dir', type=lambda x: x.lower() == 'true', required=True, help='Recreate directory before starting (true/false)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Benchmark Configuration:")
    print("=" * 60)
    print(f"Max inflight requests:        {args.max_inflight_requests}")
    print(f"Max write waiters:            {args.max_write_waiters}")
    print(f"Workers per request:          {args.num_workers_per_single_request}")
    print(f"KV base path:                 {args.kv_base_path}")
    print(f"Number of files:              {args.num_files}")
    print(f"File size:                    {args.file_size} bytes ({args.file_size / 1024 / 1024:.2f} MB)")
    print(f"Requests to complete:         {args.requests_to_complete}")
    if args.rate_limit_bytes_per_second > 0:
        print(f"Rate limit:                   {args.rate_limit_bytes_per_second} bytes/sec ({args.rate_limit_bytes_per_second / 1024 / 1024 / 1024:.2f} GB/sec)")
    else:
        print(f"Rate limit:                   Unlimited")
    print(f"File manager type:            {args.file_manager_type}")
    print("=" * 60)
    print()
    
    system = System(
        args.max_inflight_requests,
        args.max_write_waiters,
        args.num_workers_per_single_request,
        args.kv_base_path,
        args.num_files,
        args.file_size,
        args.requests_to_complete,
        args.rate_limit_bytes_per_second,
        args.file_manager_type,
        args.recreate_dir
    )
    
    asyncio.run(system.run_benchmark())

if __name__ == '__main__':
    main()
 