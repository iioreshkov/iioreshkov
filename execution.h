#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace execution {

    class Task {
    public:
        virtual ~Task() {
        }

        virtual void Run() = 0;

        void Wait() {
            std::unique_lock<std::mutex> guard(mutex_);
            while (!is_finished_.load()) {
                is_finished_cond_.wait(guard);
            }
        }

        bool IsFinished() {
            return is_finished_.load();
        }

        void SetFinished() {
            is_finished_.store(true);
            is_finished_cond_.notify_all();
        }

    private:
        std::atomic<bool> is_finished_{false};
        std::condition_variable is_finished_cond_;
        std::mutex mutex_;
    };

    class Executor {
    public:
        explicit Executor(int num_threads) {
            threads_.reserve(num_threads);
            for (int i = 0; i < num_threads; ++i) {
                threads_.emplace_back([&]() { Worker(); });
            }
        }

        ~Executor() {
            if (is_working_.load()) {
                StartShutdown();
                WaitShutdown();
            }
        }

        void Submit(std::shared_ptr<Task> task) {
            std::lock_guard<std::mutex> guard(mutex_);
            tasks_.push(task);
            que_not_empty_.notify_one();
        }

        void StartShutdown() {
            std::lock_guard<std::mutex> guard(mutex_);
            is_working_.store(false);
            que_not_empty_.notify_all();
        }

        void WaitShutdown() {
            for (auto& thread : threads_) {
                thread.join();
            }
            while (!tasks_.empty()) {
                tasks_.pop();
            }
        }

    private:
        void Worker() {
            std::unique_lock<std::mutex> guard(mutex_);
            while (is_working_.load() || !tasks_.empty()) {
                while (tasks_.empty() && is_working_.load()) {
                    que_not_empty_.wait(guard);
                }
                if (!tasks_.empty()) {
                    auto task_ptr = tasks_.front();
                    tasks_.pop();

                    guard.unlock();
                    task_ptr->Run();
                    task_ptr->SetFinished();
                    guard.lock();
                }
            }
        }

    private:
        std::atomic<bool> is_working_{true};
        std::vector<std::thread> threads_;
        std::mutex mutex_;
        std::queue<std::shared_ptr<Task>> tasks_;
        std::condition_variable que_not_empty_;
    };

    std::shared_ptr<Executor> MakeThreadPoolExecutor(int num_threads) {
        auto ptr = std::make_shared<Executor>(num_threads);
        return ptr;
    }

}