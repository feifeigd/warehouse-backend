#include "PluginManager.hpp"
#include <iostream>

int main() {
    PluginManager pm;
    pm.searchPlugins("../../plugins/hello_plugin"); // 可根据实际插件目录调整
    const auto& plugins = pm.getPlugins();
    for (const auto& p : plugins) {
        if (p) {
            std::cout << "Loaded plugin: " << p->name() << std::endl;
            p->doWork();
        }
    }
    return 0;
}
