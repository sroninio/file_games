#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <chrono>

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
    
    std::cout << "Parameters:" << std::endl;
    std::cout << "  N (number of files): " << N << std::endl;
    std::cout << "  K (file size in bytes): " << K << std::endl;
    std::cout << "  ITER (iterations): " << ITER << std::endl;
    std::cout << "  PATH (directory): " << PATH << std::endl;
    std::cout << std::endl;
    
    // Create directory if it doesn't exist
    try {
        fs::create_directories(PATH);
    } catch (const std::exception& e) {
        std::cerr << "Error creating directory: " << e.what() << std::endl;
        return 1;
    }
    
    // Step 1: Create N files, each of size K bytes
    std::cout << "Creating " << N << " files..." << std::endl;
    auto start_create = std::chrono::high_resolution_clock::now();
    
    std::vector<char> buffer(K, 'A');  // Fill buffer with 'A' characters
    // Create some variation in the data
    for (int i = 0; i < K; i++) {
        buffer[i] = 'A' + (i % 26);
    }
    
    for (int i = 1; i <= N; i++) {
        std::string filename = PATH + "/f" + std::to_string(i);
        std::ofstream file(filename, std::ios::binary);
        
        if (!file) {
            std::cerr << "Error creating file: " << filename << std::endl;
            return 1;
        }
        
        file.write(buffer.data(), K);
        file.close();
    }
    
    auto end_create = std::chrono::high_resolution_clock::now();
    auto duration_create = std::chrono::duration_cast<std::chrono::milliseconds>(end_create - start_create);
    std::cout << "Created " << N << " files in " << duration_create.count() << " ms" << std::endl;
    std::cout << std::endl;
    
    // Step 2: Perform ITER iterations
    std::cout << "Starting " << ITER << " iterations..." << std::endl;
    auto start_read = std::chrono::high_resolution_clock::now();
    
    std::vector<char> read_buffer(K);
    long long total_bytes_read = 0;
    
    for (int i = 0; i < ITER; i++) {
        // Calculate which file to read (cycle through files 1 to N)
        int file_num = (i % N) + 1;
        std::string filename = PATH + "/f" + std::to_string(file_num);
        
        // 1. Open file i
        std::ifstream file(filename, std::ios::binary);
        
        if (!file) {
            std::cerr << "Error opening file: " << filename << std::endl;
            return 1;
        }
        
        // 2. Synchronously read all content of file i
        file.read(read_buffer.data(), K);
        std::streamsize bytes_read = file.gcount();
        total_bytes_read += bytes_read;
        
        // 3. Close file i
        file.close();
        
        // Print progress every 10% of iterations
        if ((i + 1) % (ITER / 10 == 0 ? 1 : ITER / 10) == 0) {
            std::cout << "  Completed iteration " << (i + 1) << " / " << ITER << std::endl;
        }
    }
    
    auto end_read = std::chrono::high_resolution_clock::now();
    auto duration_read = std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_read);
    
    std::cout << std::endl;
    std::cout << "Completed " << ITER << " iterations in " << duration_read.count() << " ms" << std::endl;
    std::cout << "Total bytes read: " << total_bytes_read << std::endl;
    std::cout << "Average time per iteration: " 
              << (duration_read.count() / (double)ITER) << " ms" << std::endl;
    
    return 0;
}

