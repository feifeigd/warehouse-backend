#include "PluginManager.hpp"


#include <string>
#include <string_view>
using namespace std::literals;  // sv

// 平台特定的动态库加载宏
#ifdef _WIN32
    #include <windows.h>
    #define LIB_HANDLE HMODULE
    #define DLOPEN(path) LoadLibraryW(path)
    #define DLSYM(handle, symbol) GetProcAddress(handle, symbol)
    #define DLCLOSE(handle) FreeLibrary(handle)
    constexpr std::string_view PLUGIN_EXT = ".dll"sv;
#elif defined(__APPLE__) // macOS
    #include <dlfcn.h>
    #define LIB_HANDLE void*
    #define DLOPEN(path) dlopen(path, RTLD_LAZY)
    #define DLSYM(handle, symbol) dlsym(handle, symbol)
    #define DLCLOSE(handle) dlclose(handle)
    constexpr std::string_view PLUGIN_EXT = ".dylib"sv;
#else // Linux and other POSIX systems
    #include <dlfcn.h>
    #define LIB_HANDLE void*
    #define DLOPEN(path) dlopen(path, RTLD_LAZY)
    #define DLSYM(handle, symbol) dlsym(handle, symbol)
    #define DLCLOSE(handle) dlclose(handle)
    constexpr std::string_view PLUGIN_EXT = ".so"sv;
#endif

#include <vector>
#include <memory>
#include <filesystem>
#include <iostream>
using namespace std;
namespace fs = std::filesystem;


struct PluginManager::Impl {
    struct PluginHandle {
        LIB_HANDLE handle;
        std::unique_ptr<IPlugin> instance;
    };
    std::vector<PluginHandle> plugins;
};

PluginManager::PluginManager() : impl(new Impl) {}
PluginManager::~PluginManager() { unloadPlugins(); delete impl; }

std::map<std::string, std::string> PluginManager::searchPlugins(const std::string& directory) {
    unloadPlugins();
    std::map<std::string, std::string> foundPlugins;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (!entry.is_regular_file()) continue;

        const auto& path = entry.path();
        if (path.extension() != PLUGIN_EXT) continue;

        foundPlugins[path.string()] = path.string();
        
        LIB_HANDLE lib = DLOPEN(path.wstring().c_str());
        if (!lib) continue;
        auto create = (IPlugin*(*)())DLSYM(lib, "create_plugin");
        if (!create) {
            DLCLOSE(lib);
            continue;
        }
        std::unique_ptr<IPlugin> plugin(create());
        if (plugin) {
            Impl::PluginHandle handle;
            handle.handle = lib;
            handle.instance = std::move(plugin);
            impl->plugins.push_back(std::move(handle));
        }
    }
    return foundPlugins;
}

const std::vector<std::unique_ptr<IPlugin>>& PluginManager::getPlugins() const {
    static std::vector<std::unique_ptr<IPlugin>> refs;
    refs.clear();
    for (const auto& h : impl->plugins) {
        refs.push_back(std::unique_ptr<IPlugin>(h.instance.get()));
    }
    return refs;
}

void PluginManager::unloadPlugins() {
    for (auto& h : impl->plugins) {
        h.instance.reset();
        if (h.handle) DLCLOSE(h.handle);
    }
    impl->plugins.clear();
}
