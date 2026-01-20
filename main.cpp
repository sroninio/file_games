#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cerrno>
#include <algorithm>
#include <random>
#include <thread>
#include <atomic>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    // Parameters
    int N = 10;           // Number of files
    long long K = 1024;   // Size of each file in bytes (1 KB)
    int ITER = 100;       // Number of iterations
    std::string PATH = "./test_files";  // Directory path
    bool CREATE_DELETE_MODE = true;  // If true: delete and create files; if false: use existing files
    bool DROP_CACHE_INITIAL = false;  // If true: drop cache at the beginning (requires root)
    bool SKIP_READ = false;  // If true: only open/close files, skip the read operation
    bool SKIP_WRITE = false;  // If true: create files but skip writing data (empty files)
    size_t CHUNK_SIZE = 4 * 1024 * 1024;  // Chunk size for reading (4 MB default)
    bool PARALLEL_READ = false;  // If true: issue parallel reads for all chunks
    
    // Parse command line arguments if provided
    if (argc >= 2) N = std::stoi(argv[1]);
    if (argc >= 3) K = std::stoll(argv[2]);
    if (argc >= 4) ITER = std::stoi(argv[3]);
    if (argc >= 5) PATH = argv[4];
    if (argc >= 6) CREATE_DELETE_MODE = (std::string(argv[5]) == "1" || std::string(argv[5]) == "true");
    if (argc >= 7) DROP_CACHE_INITIAL = (std::string(argv[6]) == "1" || std::string(argv[6]) == "true");
    if (argc >= 8) SKIP_READ = (std::string(argv[7]) == "1" || std::string(argv[7]) == "true");
    if (argc >= 9) SKIP_WRITE = (std::string(argv[8]) == "1" || std::string(argv[8]) == "true");
    if (argc >= 10) CHUNK_SIZE = std::stoull(argv[9]);
    if (argc >= 11) PARALLEL_READ = (std::string(argv[10]) == "1" || std::string(argv[10]) == "true");
    
    // Assume K is already aligned to 4096 bytes
    const size_t ALIGNMENT = 4096;
    long long aligned_K = K;
    
    std::cout << "Parameters:" << std::endl;
    std::cout << "  N (number of files): " << N << std::endl;
    std::cout << "  K (file size in bytes): " << K << std::endl;
    std::cout << "  ITER (iterations): " << ITER << std::endl;
    std::cout << "  PATH (directory): " << PATH << std::endl;
    std::cout << "  CHUNK_SIZE (read chunk size): " << CHUNK_SIZE << " bytes" << std::endl;
    std::cout << "  PARALLEL_READ: " << (PARALLEL_READ ? "enabled" : "disabled (sequential)") << std::endl;
    std::cout << "  CREATE_DELETE_MODE: " << (CREATE_DELETE_MODE ? "enabled (delete and create files)" : "disabled (use existing files)") << std::endl;
    std::cout << "  DROP_CACHE_INITIAL: " << (DROP_CACHE_INITIAL ? "enabled (requires root)" : "disabled") << std::endl;
    std::cout << "  SKIP_READ: " << (SKIP_READ ? "enabled (only open/close)" : "disabled (full read)") << std::endl;
    std::cout << "  SKIP_WRITE: " << (SKIP_WRITE ? "enabled (create empty files)" : "disabled (write data)") << std::endl;
    std::cout << std::endl;
    
    if (CREATE_DELETE_MODE) {
        // Delete all content in PATH directory if it exists
        try {
            if (fs::exists(PATH)) {
                std::cout << "Removing existing directory and all its contents..." << std::endl;
                fs::remove_all(PATH);
                std::cout << "Directory cleaned." << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error removing directory: " << e.what() << std::endl;
            return 1;
        }
        
        // Create directory
        try {
            fs::create_directories(PATH);
            std::cout << "Created directory: " << PATH << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error creating directory: " << e.what() << std::endl;
            return 1;
        }
        std::cout << std::endl;
        
        // Step 1: Create N files, each of size aligned_K bytes
        std::cout << "Creating " << N << " files..." << std::endl;
        auto start_create = std::chrono::high_resolution_clock::now();
        
        for (int i = 1; i <= N; i++) {
            std::string filename = PATH + "/f" + std::to_string(i);
            
            if (!SKIP_WRITE) {
                // Use dd to copy from /dev/urandom directly to file (no intermediate buffer in our code)
                // Since aligned_K is already aligned to 4096 bytes, use bs=4096
                std::string cmd = "dd if=/dev/urandom of=\"" + filename + 
                                  "\" bs=4096 count=" + std::to_string(aligned_K / 4096) +
                                  " iflag=fullblock status=none 2>&1";
                int ret = system(cmd.c_str());
                if (ret != 0) {
                    std::cerr << "Error creating file with dd: " << filename << std::endl;
                    return 1;
                }
            } else {
                // Just create empty file
                int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (fd == -1) {
                    std::cerr << "Error creating file: " << filename << std::endl;
                    return 1;
                }
                close(fd);
            }
            
            // Print progress every 1000 files
            if (i % 1000 == 0) {
                auto current_time = std::chrono::high_resolution_clock::now();
                auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_create);
                double avg_time = elapsed_ms.count() / (double)i;
                std::cout << "  Created " << i << " files, avg time per file: " 
                          << avg_time << " ms" << std::endl;
            }
        }
        
        auto end_create = std::chrono::high_resolution_clock::now();
        auto duration_create = std::chrono::duration_cast<std::chrono::milliseconds>(end_create - start_create);
        if (SKIP_WRITE) {
            std::cout << "Created " << N << " files (without writing data) in " << duration_create.count() << " ms" << std::endl;
        } else {
            std::cout << "Created " << N << " files in " << duration_create.count() << " ms" << std::endl;
        }
        std::cout << std::endl;
    }
    
    // Drop cache at the beginning if requested
    if (DROP_CACHE_INITIAL) {
        std::cout << "Dropping all caches (requires root privileges)..." << std::endl;
        std::ofstream drop_cache("/proc/sys/vm/drop_caches");
        if (drop_cache) {
            drop_cache << "3" << std::endl;  // Drop all caches
            drop_cache.close();
            std::cout << "Cache dropped successfully." << std::endl;
        } else {
            std::cerr << "Error: Could not drop cache. Need root privileges (run with sudo)." << std::endl;
            return 1;
        }
        std::cout << std::endl;
    }
    
    // Step 2: Perform ITER iterations with O_DIRECT
    if (SKIP_READ) {
        std::cout << "Starting " << ITER << " iterations (open/close only)..." << std::endl;
    } else if (PARALLEL_READ) {
        long long num_chunks = aligned_K / CHUNK_SIZE;
        std::cout << "Starting " << ITER << " iterations with O_DIRECT (parallel: " 
                  << num_chunks << " threads per file)..." << std::endl;
    } else {
        std::cout << "Starting " << ITER << " iterations with O_DIRECT..." << std::endl;
    }
    
    // Use chunk-based reading for large files
    // Allocate aligned buffer for O_DIRECT (only for one chunk at a time)
    void* read_buffer_raw;
    if (posix_memalign(&read_buffer_raw, ALIGNMENT, CHUNK_SIZE) != 0) {
        std::cerr << "Error allocating aligned buffer" << std::endl;
        return 1;
    }
    char* read_buffer = static_cast<char*>(read_buffer_raw);
    
    // Create a permutation of file indices 1..N
    std::vector<int> file_permutation(N);
    for (int i = 1; i <= N; i++) {
        file_permutation[i - 1] = i;  // Files are numbered 1 to N
    }
    // Shuffle the permutation for random access pattern
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(file_permutation.begin(), file_permutation.end(), gen);
    
    std::cout << "Created random permutation of " << N << " files" << std::endl;
    
    auto start_read = std::chrono::high_resolution_clock::now();
    long long total_bytes_read = 0;
    
    for (int i = 0; i < ITER; i++) {
        // Use permutation to access files in random order
        int file_num = file_permutation[i % N];
        std::string filename = PATH + "/f" + std::to_string(file_num);
        
        // 1. Open file i with O_DIRECT flag
        int fd = open(filename.c_str(), O_RDONLY | O_DIRECT);
        
        if (fd == -1) {
            std::cerr << "Error opening file with O_DIRECT: " << filename 
                      << " (errno: " << errno << ")" << std::endl;
            // Try without O_DIRECT as fallback
            fd = open(filename.c_str(), O_RDONLY);
            if (fd == -1) {
                std::cerr << "Error opening file: " << filename << std::endl;
                free(read_buffer);
                return 1;
            }
            std::cout << "Warning: O_DIRECT not supported, reading without it" << std::endl;
        }
        
        // 2. Read all content of file i
        ssize_t file_total_read = 0;
        
        if (!SKIP_READ) {
            if (PARALLEL_READ) {
                // Parallel reading: issue file_size/chunk_size parallel reads
                long long num_chunks = aligned_K / CHUNK_SIZE;
                std::atomic<bool> error_occurred(false);
                std::atomic<long long> total_read(0);
                std::vector<std::thread> threads;
                
                for (long long chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
                    threads.emplace_back([&, chunk_idx]() {
                        off_t offset = chunk_idx * CHUNK_SIZE;
                        
                        // Allocate thread-local aligned buffer
                        void* thread_buffer_raw;
                        if (posix_memalign(&thread_buffer_raw, ALIGNMENT, CHUNK_SIZE) != 0) {
                            error_occurred = true;
                            return;
                        }
                        char* thread_buffer = static_cast<char*>(thread_buffer_raw);
                        
                        // Use pread to read from specific offset
                        ssize_t bytes_read = pread(fd, thread_buffer, CHUNK_SIZE, offset);
                        if (bytes_read != (ssize_t)CHUNK_SIZE) {
                            error_occurred = true;
                        } else {
                            total_read += bytes_read;
                        }
                        
                        free(thread_buffer);
                    });
                }
                
                // Wait for all threads to complete
                for (auto& thread : threads) {
                    thread.join();
                }
                
                if (error_occurred) {
                    std::cerr << "Error in parallel read of file " << filename << std::endl;
                    close(fd);
                    free(read_buffer);
                    return 1;
                }
                
                file_total_read = total_read.load();
            } else {
                // Sequential reading
                size_t file_remaining = aligned_K;
                while (file_remaining > 0) {
                    size_t to_read = (file_remaining < CHUNK_SIZE) ? file_remaining : CHUNK_SIZE;
                    
                    ssize_t bytes_read = read(fd, read_buffer, to_read);
                    if (bytes_read < 0) {
                        std::cerr << "Error reading file " << filename 
                                  << " (errno: " << errno << ")" << std::endl;
                        close(fd);
                        free(read_buffer);
                        return 1;
                    }
                    if (bytes_read == 0) {
                        break;  // EOF
                    }
                    
                    file_total_read += bytes_read;
                    file_remaining -= bytes_read;
                }
            }
        }
        
        total_bytes_read += file_total_read;
        
        // 3. Close file i
        close(fd);
        
        // Print progress every 1000 iterations
        if ((i + 1) % 1000 == 0) {
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_read);
            double avg_time = elapsed_ms.count() / (double)(i + 1);
            std::cout << "  Completed " << (i + 1) << " iterations, avg time per iteration: " 
                      << avg_time << " ms" << std::endl;
        }
    }
    
    // Free aligned buffer
    free(read_buffer);
    
    auto end_read = std::chrono::high_resolution_clock::now();
    auto duration_read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_read);
    auto duration_read_sec = std::chrono::duration_cast<std::chrono::seconds>(end_read - start_read);
    
    std::cout << std::endl;
    std::cout << "Completed " << ITER << " iterations" << std::endl;
    std::cout << "Total time: " << duration_read_sec.count() << " seconds (" 
              << duration_read_ms.count() << " ms)" << std::endl;
    std::cout << "Total bytes read: " << total_bytes_read << std::endl;
    std::cout << "Average time per iteration: " 
              << (duration_read_ms.count() / (double)ITER) << " ms" << std::endl;
    
    return 0;
}

