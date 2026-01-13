#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    // Parameters
    int N = 10;           // Number of files
    int K = 1024;         // Size of each file in bytes (1 KB)
    int ITER = 100;       // Number of iterations
    std::string PATH = "./test_files";  // Directory path
    
    // Parse command line arguments if provided
    if (argc >= 2) N = std::stoi(argv[1]);
    if (argc >= 3) K = std::stoi(argv[2]);
    if (argc >= 4) ITER = std::stoi(argv[3]);
    if (argc >= 5) PATH = argv[4];
    
    // Align K to block size for O_DIRECT compatibility
    const int BLOCK_SIZE = 512;
    int aligned_K = ((K + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
    
    std::cout << "Parameters:" << std::endl;
    std::cout << "  N (number of files): " << N << std::endl;
    std::cout << "  K (file size in bytes): " << K << std::endl;
    if (aligned_K != K) {
        std::cout << "  K adjusted to " << aligned_K << " bytes for O_DIRECT alignment" << std::endl;
    }
    std::cout << "  ITER (iterations): " << ITER << std::endl;
    std::cout << "  PATH (directory): " << PATH << std::endl;
    std::cout << std::endl;
    
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
    
    std::vector<char> buffer(aligned_K, 'A');  // Fill buffer with 'A' characters
    // Create some variation in the data
    for (int i = 0; i < aligned_K; i++) {
        buffer[i] = 'A' + (i % 26);
    }
    
    for (int i = 1; i <= N; i++) {
        std::string filename = PATH + "/f" + std::to_string(i);
        std::ofstream file(filename, std::ios::binary);
        
        if (!file) {
            std::cerr << "Error creating file: " << filename << std::endl;
            return 1;
        }
        
        file.write(buffer.data(), aligned_K);
        file.close();
    }
    
    auto end_create = std::chrono::high_resolution_clock::now();
    auto duration_create = std::chrono::duration_cast<std::chrono::milliseconds>(end_create - start_create);
    std::cout << "Created " << N << " files in " << duration_create.count() << " ms" << std::endl;
    std::cout << std::endl;
    
    // Step 2: Perform ITER iterations with O_DIRECT
    std::cout << "Starting " << ITER << " iterations with O_DIRECT..." << std::endl;
    
    // Allocate aligned buffer for O_DIRECT
    void* read_buffer_raw;
    if (posix_memalign(&read_buffer_raw, BLOCK_SIZE, aligned_K) != 0) {
        std::cerr << "Error allocating aligned buffer" << std::endl;
        return 1;
    }
    char* read_buffer = static_cast<char*>(read_buffer_raw);
    
    auto start_read = std::chrono::high_resolution_clock::now();
    long long total_bytes_read = 0;
    
    for (int i = 0; i < ITER; i++) {
        // Calculate which file to read (cycle through files 1 to N)
        int file_num = (i % N) + 1;
        std::string filename = PATH + "/f" + std::to_string(file_num);
        
        // 1. Open file i with O_DIRECT flag
        int fd = open(filename.c_str(), O_RDONLY | O_DIRECT);
        
        if (fd == -1) {
            std::cerr << "Error opening file: " << filename << std::endl;
            free(read_buffer);
            return 1;
        }
        
        // 2. Synchronously read all content of file i
        ssize_t bytes_read = read(fd, read_buffer, aligned_K);
        if (bytes_read > 0) {
            total_bytes_read += bytes_read;
        }
        
        // 3. Close file i
        close(fd);
        
        // Print progress every 10% of iterations
        if ((i + 1) % (ITER / 10 == 0 ? 1 : ITER / 10) == 0) {
            std::cout << "  Completed iteration " << (i + 1) << " / " << ITER << std::endl;
        }
    }
    
    // Free aligned buffer
    free(read_buffer);
    
    auto end_read = std::chrono::high_resolution_clock::now();
    auto duration_read = std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_read);
    
    std::cout << std::endl;
    std::cout << "Completed " << ITER << " iterations in " << duration_read.count() << " ms" << std::endl;
    std::cout << "Total bytes read: " << total_bytes_read << std::endl;
    std::cout << "Average time per iteration: " 
              << (duration_read.count() / (double)ITER) << " ms" << std::endl;
    
    return 0;
}

