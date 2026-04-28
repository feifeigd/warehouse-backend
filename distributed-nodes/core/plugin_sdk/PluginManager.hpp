#pragma once
#include "hello_main/IPlugin.hpp"
#include <vector>
#include <map>
#include <string>
#include <memory>

class PluginManager {
public:
    PluginManager();
    ~PluginManager();
    // 搜索指定目录下所有插件
    std::map<std::string, std::string> searchPlugins(const std::string& directory);
    // 获取所有插件实例
    const std::vector<std::unique_ptr<IPlugin>>& getPlugins() const;
    // 卸载所有插件
    void unloadPlugins();
private:
    struct Impl;
    Impl* impl;
};
