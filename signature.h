#pragma once

#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "execution.h"

namespace signature {

    class Reader {
    public:
        explicit Reader(const std::string& file_name, int buffer_size) : buffer_size_(buffer_size) {
            try {
                input_stream_.open(file_name, std::ios::binary);
            } catch (std::exception& ex) {
                throw std::runtime_error("Failed to open input file.");
            }
            if(input_stream_.fail()) {
                throw std::runtime_error("No file.");
            }
        }

        ~Reader() {
            input_stream_.close();
        }

        bool ReadChunk() {
            try {
                buffer_.assign(buffer_size_, 0);
            } catch (std::exception& ex) {
                throw std::runtime_error("Can not allocate memory for a new batch...");
            }
            input_stream_.read(&buffer_[0], buffer_size_);
            if (input_stream_.bad()) {
                throw std::runtime_error("Failed reading new batch.");
            }
            return input_stream_.gcount() > 0;
        }

        std::string GetChunk() {
            return std::move(buffer_);
        }

    private:
        int buffer_size_ = 0;
        std::ifstream input_stream_;
        std::string buffer_;
    };

    class Writer {
    public:
        explicit Writer(const std::string& file_name) {
            try {
                output_stream_.open(file_name, std::ios::binary);
            } catch (std::exception& ex) {
                throw std::runtime_error("Failed to create output file.");
            }
        }

        ~Writer() {
            output_stream_.close();
        }

        void WriteHash(size_t hash_val) {
            output_stream_.write(reinterpret_cast<char*>(&hash_val), sizeof(hash_val));
            if (output_stream_.bad()) {
                throw std::runtime_error("Failed write hash value to file.");
            }
        }

    private:
        std::ofstream output_stream_;
    };

    class HashingTask : public execution::Task {
    public:
        explicit HashingTask(std::string&& bucket) : bucket_(std::move(bucket)) {}

        void Run() override {
            hash_value_ = std::hash<std::string>()(bucket_);
            bucket_.clear();
            bucket_.shrink_to_fit();
        }

        size_t GetHash() const {
            return hash_value_;
        }

    private:
        size_t hash_value_ = 0;
        std::string bucket_;
    };

    class WritingTask : public execution::Task {
    public:
        explicit WritingTask(std::shared_ptr<HashingTask> hashing_task, std::shared_ptr<Writer> writer)
                : hashing_task_(std::move(hashing_task)), writer_(writer) {
        }

        void Run() override {
            hashing_task_->Wait();
            writer_->WriteHash(hashing_task_->GetHash());
            std::cout << hashing_task_->GetHash() << '\n';
        }

    private:
        std::shared_ptr<HashingTask> hashing_task_;
        std::shared_ptr<Writer> writer_;
    };

    struct Params {
        static const size_t kMBtoBytes = 1024uLL * 1024uLL;
        std::string input_file_name;
        std::string output_file_name;
        size_t batchSize = 1ull * kMBtoBytes;
        bool is_success = false;
        std::string error_message;
    };

    Params ParseArguments(int argc, char* argv[]) {
        Params params;
        const auto kDefaultArgNum = 3;
        const auto kArgWithBatchSize = 4;

        if (argc == kDefaultArgNum) {
            params.input_file_name = argv[1];
            params.output_file_name = argv[2];
            params.is_success = true;
        } else if (argc == kArgWithBatchSize) {
            size_t input_batch_size;
            try {
                input_batch_size = std::stoi(argv[3]);
            } catch (std::exception& ex) {
                params.error_message = "Bad batch size input: " + std::string(argv[3]);
                return params;
            }

            if (input_batch_size < 1 || input_batch_size > 4000) {
                params.error_message = "Error: Wrong blocksize. :(\nPlease set another one...\n";
            } else {
                params.batchSize = input_batch_size * Params::kMBtoBytes;
                params.input_file_name = argv[1];
                params.output_file_name = argv[2];
                params.is_success = true;
            }
        } else {
            params.error_message = "Error: wrong number of arguments.\nUse this syntax: <input file> <output file> <block size>(default 1Mb)";
        }
        return params;
    }

    void CalculateSignature(const Params& params) {
        Reader reader(params.input_file_name, params.batchSize);
        auto writer = std::make_shared<Writer>(params.output_file_name);

        int number_threads = static_cast<int>(std::thread::hardware_concurrency());

        const int hashing_pool_size = std::max<int>(number_threads - 2, 1);
        const int writing_pool_size = 1;

        auto hashing_pool = execution::MakeThreadPoolExecutor(hashing_pool_size);
        auto writing_pool = execution::MakeThreadPoolExecutor(writing_pool_size);

        while(reader.ReadChunk()) {
            auto hashing_task = std::make_shared<HashingTask>(reader.GetChunk());
            auto writing_task = std::make_shared<WritingTask>(hashing_task, writer);
            hashing_pool->Submit(hashing_task);
            writing_pool->Submit(writing_task);
        }
    }

}