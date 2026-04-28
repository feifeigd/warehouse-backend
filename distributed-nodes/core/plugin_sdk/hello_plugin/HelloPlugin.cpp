#include "../hello_main/IPlugin.hpp"
#include <iostream>
#include <string>

class HelloPlugin : public IPlugin {
public:
    std::string name() const override { return "HelloPlugin"; }
    void doWork() override { std::cout << "Hello from plugin!\n"; }
};

extern "C" IPlugin* create_plugin() {
    return new HelloPlugin();
}
