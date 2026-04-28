#pragma once
#include <vector>
#include <string>
#include <memory>
#include "hello_main/IPlugin.hpp"

class PluginManager {
public:
    PluginManager();
    ~PluginManager();
    // 加载指定目录下所有插件
    void loadPlugins(const std::string& directory);
    // 获取所有插件实例
    const std::vector<std::unique_ptr<IPlugin>>& getPlugins() const;
    // 卸载所有插件
    void unloadPlugins();
private:
    struct Impl;
    Impl* impl;
};
