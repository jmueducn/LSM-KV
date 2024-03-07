#include <iostream>
#include <cstdint>
#include <string>
#include <chrono>
#include <functional> // Add this line
#include "test.h"

class CorrectnessTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1024 * 12;
    const int NUM_ITERATIONS = 1;

    // Helper function to measure average time delay
    double measureAverageTimeDelay(const std::function<void()>& func)
    {
        std::chrono::high_resolution_clock::time_point start, end;
        double totalDelay = 0.0;

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            start = std::chrono::high_resolution_clock::now();
            func();
            end = std::chrono::high_resolution_clock::now();
            totalDelay += std::chrono::duration<double, std::micro>(end - start).count();
        }

        return totalDelay / NUM_ITERATIONS;
    }

    void regular_test(uint64_t max)
    {
        uint64_t i;

        // Test a single key
        EXPECT(not_found, store.get(1));
        store.put(1, "SE");
        EXPECT("SE", store.get(1));
        EXPECT(true, store.del(1));
        EXPECT(not_found, store.get(1));
        EXPECT(false, store.del(1));

        phase();
	double totalTime = 0.0;
	double totalPutDelay=0;
	int lasti=0;
// Test multiple key-value pairs
/*for (i = 0; i < max; ++i) {
    double putDelay = measureAverageTimeDelay([&]() {
        store.put(i, std::string(10000, 's'));
    });
    totalPutDelay += putDelay;
    totalTime += putDelay;

    if (totalTime >= 1000000) {  // 每秒钟
        double averagePutDelay = totalPutDelay / (i-lasti);  // 平均延迟
        double putThroughput = (i-lasti) / totalTime;  // 吞吐量
        std::cout << "Average PUT latency for the past second: " << averagePutDelay << " microseconds" << std::endl;
        std::cout << "PUT throughput for the past second: " << putThroughput*100000 << " operations/second" << std::endl;
        std::cout << i << std::endl;
        totalTime = 0.0;
        totalPutDelay = 0.0;
        lasti=i;
    }
}*/
        for (i = 0; i < max; ++i) {
            double putDelay = measureAverageTimeDelay([&]() {
                store.put(i, std::string(i+1, 's'));
            });
            totalPutDelay += putDelay;
        }
	double averagePutDelay = totalPutDelay / max;
        std::cout << "Average GET tp: " << 1000000/averagePutDelay << " " << std::endl;
       
        phase();
	double totalGetDelay=0;
        for (i = 0; i < max; ++i) {
            double getDelay = measureAverageTimeDelay([&]() {
                EXPECT(std::string(i + 1, 's'), store.get(i));
            });
            totalGetDelay += getDelay;
        }

        double averageGetDelay = totalGetDelay / max;
        std::cout << "Average GET tp: " << 1000000/averageGetDelay << " " << std::endl;

        phase();
	double totalDelDelay=0;
        for (i = 0; i < max; i += 2) {
            double delDelay = measureAverageTimeDelay([&]() {
                EXPECT(true, store.del(i));
            });
            totalDelDelay += delDelay;
        }

        double averageDelDelay = totalDelDelay / (max / 2);
        std::cout << "Average DELETE tp: " << 1000000/averageDelDelay << " " << std::endl;


        phase();

        report();
    }

public:
    CorrectnessTest(const std::string& dir, bool v = true) : Test(dir, v)
    {
    }

    void start_test(void* args = NULL) override
    {
        std::cout << "KVStore Correctness Test" << std::endl;

        store.reset();

        std::cout << "[Simple Test]" << std::endl;
        regular_test(SIMPLE_TEST_MAX);

        store.reset();

        std::cout << "[Large Test]" << std::endl;
        regular_test(LARGE_TEST_MAX);
    }
};

int main(int argc, char* argv[])
{
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");
    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();
    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}

