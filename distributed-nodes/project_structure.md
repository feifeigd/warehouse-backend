# 分布式 CAF 项目推荐目录结构

```text
distributed-nodes/
├── core/                # 框架核心（通用基础设施/抽象/工具类/协议/actor管理等）
│   ├── actor_manager.hpp
│   ├── rpc_client.hpp
│   ├── rpc_server.hpp
│   ├── message_types.hpp
│   ├── serialization.hpp
│   └── ...（如 util, config, logging, etc.）
├── services/            # 具体业务服务（如 region、compute、storage、master 等）
│   ├── region/
│   │   ├── region_actor.cpp
│   │   └── ...
│   ├── compute/
│   ├── storage/
│   └── master/
├── app/                 # 应用入口（main、命令行、集成/组合各服务）
│   ├── client_main.cpp
│   ├── master_main.cpp
│   ├── region_main.cpp
│   └── ...
├── config/              # 配置文件（*.conf、*.json、*.yaml 等）
├── scripts/             # 启动、测试、部署脚本
├── tests/               # 单元测试、集成测试
└── CMakeLists.txt       # 构建入口
```

## 组织与迁移建议

1. **core/**：将 remote_actor_manager.hpp、rpc 相关、通用工具、协议、序列化等基础设施代码移入 core/。
2. **services/**：每个业务模块（region、compute、storage、master）单独子目录，聚合相关 actor、业务逻辑、状态等。
3. **app/**：只做组装和启动，main 函数、参数解析、服务组合等。
4. **config/**、**scripts/**、**tests/**：分别存放配置、脚本和测试。

## CMakeLists.txt 修改建议

- 根目录 CMakeLists.txt 只负责 add_subdirectory(core) add_subdirectory(services/region) ... add_subdirectory(app) add_subdirectory(tests) 等。
- core/、services/、app/、tests/ 各自有独立 CMakeLists.txt，管理本目录下源文件和依赖。
- 测试目标只在 tests/ 下定义，且不参与主程序发布。

---

如需具体迁移脚本或 CMakeLists.txt 示例，可继续提问。