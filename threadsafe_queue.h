#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <thread>

template<typename T>
class threadsafe_queue{
private:
    mutable std::mutex mutex;
    std::queue<T> data_queue;
    std::condition_variable cond;

public:
    threadsafe_queue(){}
    threadsafe_queue(threadsafe_queue const& other){
        std::lock_guard<std::mutex> lk(other.mutex);
        data_queue = other.data_queue();
    }
    void push(T new_value){
        std::lock_guard<std::mutex> lk(mutex);
        data_queue.push(new_value);
        cond.notify_one();
    }
    void wait_and_pop(T& value){
        std::unique_lock<std::mutex> lk(mutex);
        cond.wait(lk, [this]{return !data_queue.empty();});
        value = data_queue.front();
        data_queue.pop();
    }

    std::shared_ptr<T> wait_and_pop(){
        std::unique_lock<std::mutex> lk(mutex);
        cond.wait(lk, [this]{return !data_queue.empty();});
        std::shared_ptr<T> res(std::make_shared<T>(data_queue.front()));
        data_queue.pop();
        return res;
    }

    bool empty()const{
        std::lock_guard<std::mutex> lk(mutex);
        return data_queue.empty();
    }
};