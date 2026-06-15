#include <iostream>
#include <future>
#include <thread>
#include <vector>

int main() {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    std::thread t([&p]() {
        p.set_value(true);
    });
    std::cout << f.get() << std::endl;
    t.join();
    return 0;
}
