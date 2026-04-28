#include "PluginManager.hpp"
#include <vector>
#include <string>
#include <memory>
#include <filesystem>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

using namespace std;
namespace fs = std::filesystem;

struct PluginManager::Impl {
    struct PluginHandle {
#ifdef _WIN32
        HMODULE handle = nullptr;
#else
        void* handle = nullptr;
#endif
        std::unique_ptr<IPlugin> instance;
    };
    std::vector<PluginHandle> plugins;
};

PluginManager::PluginManager() : impl(new Impl) {}
PluginManager::~PluginManager() { unloadPlugins(); delete impl; }

void PluginManager::loadPlugins(const std::string& directory) {
    unloadPlugins();
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (!entry.is_regular_file()) continue;
        const auto& path = entry.path();
#ifdef _WIN32
        if (path.extension() != ".dll") continue;
        HMODULE lib = LoadLibraryW(path.wstring().c_str());
        if (!lib) continue;
        auto create = (IPlugin*(*)())GetProcAddress(lib, "create_plugin");
#else
        if (path.extension() != ".so" && path.extension() != ".dylib") continue;
        void* lib = dlopen(path.c_str(), RTLD_LAZY);
        if (!lib) continue;
        auto create = (IPlugin*(*)())dlsym(lib, "create_plugin");
#endif
        if (!create) {
#ifdef _WIN32
            FreeLibrary(lib);
#else
            dlclose(lib);
#endif
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
#ifdef _WIN32
        if (h.handle) FreeLibrary(h.handle);
#else
        if (h.handle) dlclose(h.handle);
#endif
    }
    impl->plugins.clear();
}
