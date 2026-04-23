
#include <caf/caf_main.hpp>

#include <chrono>
#include <thread>

using namespace std::literals;

namespace {
	std::atomic<bool> shutdown_flag;
	void set_shutdown_flag(int) {
		shutdown_flag = true;
	}
}

int caf_main(caf::actor_system& sys){
	while (!shutdown_flag) {
		std::this_thread::sleep_for(100ms);
	}

    return EXIT_SUCCESS;
}

CAF_MAIN()
