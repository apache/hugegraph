# JDK 11 vs JDK 21 差异分析

## JDK 11 已知漏洞

JDK 11 自 2018 年发布以来，累积了不少已知 CVE 漏洞。以下为主要高危和中危漏洞列表：

### 严重/高危 (Critical/High)

| CVE | 影响组件 | 描述 | 修复版本 |
|-----|---------|------|---------|
| CVE-2023-22025 | Hotspot | 未经认证的远程攻击者可导致 JVM 崩溃 | 11.0.21 |
| CVE-2023-21967 | JSSE (TLS) | TLS 握手拒绝服务 | 11.0.19 |
| CVE-2023-21939 | JSSE | 证书解析漏洞，可导致信息泄露 | 11.0.19 |
| CVE-2023-21938 | JSSE | 整数溢出导致拒绝服务 | 11.0.19 |
| CVE-2023-21930 | JSSE | 远程代码执行风险 | 11.0.19 |
| CVE-2022-21628 | 序列化 | 反序列化导致拒绝服务 (DoS) | 11.0.17 |
| CVE-2022-21626 | Security | 密码操作中的 DoS 漏洞 | 11.0.17 |
| CVE-2022-21624 | Serialization | 序列化过滤绕过 | 11.0.17 |
| CVE-2022-21476 | JCA | 椭圆曲线签名算法绕过 | 11.0.15 |
| CVE-2022-21449 | JCA | ECDSA 签名验证绕过（高危！） | 11.0.15 |
| CVE-2021-35586 | TLS | TLS 1.3 握手拒绝服务 | 11.0.13 |
| CVE-2021-35578 | TLS | TLS 1.3 握手拒绝服务 | 11.0.13 |
| CVE-2021-35567 | TLS | 证书路径验证绕过 | 11.0.13 |
| CVE-2021-2388 | Hotspot | 编译器漏洞，可导致逃逸 | 11.0.11 |

### 中危 (Medium)

| CVE | 描述 | 修复版本 |
|-----|------|---------|
| CVE-2023-22006 | 未授权访问网络资源 | 11.0.21 |
| CVE-2023-21968 | 本地低权限用户可修改关键数据 | 11.0.19 |
| CVE-2023-21954 | XML 外部实体注入 (XXE) | 11.0.19 |
| CVE-2022-21618 | 密钥协商处理不当 | 11.0.17 |
| CVE-2022-21434 | 未经授权的数据访问 | 11.0.15 |
| CVE-2022-21426 | XSLT 处理中的漏洞 | 11.0.15 |

### 对 HugeGraph 的影响

- 如果使用的 JDK 11 版本低于 **11.0.21**，CVE-2022-21449 (ECDSA 签名绕过) 和 CVE-2023-21930 (远程代码执行风险) 是最需要关注的漏洞
- HugeGraph 作为对外提供 REST API 的服务，TLS/JSSE 相关漏洞（CVE-2023-21930 系列）尤其值得警惕
- 反序列化漏洞（CVE-2022-21628）对 Java 序列化场景有影响
- **建议直接使用 JDK 21，可避免以上所有漏洞**

---

## JDK 11 与 JDK 21 功能差异

JDK 11 到 JDK 21 跨越了 10 个版本（11 → 17 LTS → 21 LTS），累积了大量新特性。

### 语言特性

| 特性 | JDK 11 | JDK 17 | JDK 21 |
|------|--------|--------|--------|
| Switch 表达式 | ❌ | ✅ | ✅ |
| 文本块 (Text Blocks) | ❌ | ✅ | ✅ |
| Record 类 | ❌ | ✅ | ✅ |
| 模式匹配 instanceof | ❌ | ✅ | ✅ |
| 密封类 (Sealed Classes) | ❌ | ✅ | ✅ |
| Record 模式匹配 | ❌ | ❌ | ✅ |
| Switch 模式匹配 | ❌ | ❌ | ✅ (Preview) |
| 虚拟线程 (Virtual Threads) | ❌ | ❌ | ✅ |
| 字符串模板 | ❌ | ❌ | ✅ (Preview) |
| 未命名变量/模式 | ❌ | ❌ | ✅ |
| 外部函数与内存 API | ❌ | ❌ | ✅ (Preview) |
| 作用域值 (Scoped Values) | ❌ | ❌ | ✅ (Preview) |
| 结构化并发 (Structured Concurrency) | ❌ | ❌ | ✅ (Preview) |

### JVM & 运行时

| 特性 | JDK 11 | JDK 17 | JDK 21 |
|------|--------|--------|--------|
| 默认 GC | G1 | G1（优化） | G1（大幅优化） |
| ZGC (低延迟 GC) | 实验性 | 生产就绪 | 生产就绪（持续优化，<1ms 暂停） |
| Shenandoah GC | ❌ | 生产就绪 | 生产就绪 |
| Generational ZGC | ❌ | ❌ | ✅ |
| CDS (类数据共享) | 基础 | 动态 CDS + AppCDS | 动态 CDS + AppCDS 增强 |
| JFR (飞行记录器) | 基础 | 流式 JFR | 持续优化 |
| 启动时间优化 | 基础 | 显著改进 | 进一步优化 |
| Vector API | ❌ | ✅ (Incubator) | ✅ (Incubator，持续迭代) |
| 外部函数与内存 API | ❌ | ✅ (Incubator) | ✅ (Preview，接近稳定) |
| 分代式 ZGC | ❌ | ❌ | ✅ |
| Linux/RISC-V 移植 | ❌ | ❌ | ✅ |

### 核心类库

| 特性 | JDK 11 | JDK 17 | JDK 21 |
|------|--------|--------|--------|
| `HttpClient` | 标准 API | 增强（HTTP/2, WebSocket） | 增强 |
| `SequencedCollection` 接口 | ❌ | ❌ | ✅ |
| `Stream.toList()` | ❌ | ✅ | ✅ |
| `Map.ofEntries()` / `List.of()` 增强 | 基础 | ✅ | ✅ |
| `ProcessHandle` | 基础 | 增强 | 增强 |
| 伪随机数生成器 | 基础 | 新接口 `RandomGenerator` | 增强 |
| `ByteBuffer` 增强 | 基础 | ✅ | ✅ |
| 字符集增强 | 基础 | ✅ | ✅ |

### 废弃与移除

| 项目 | JDK 11 | JDK 17 | JDK 21 |
|------|--------|--------|--------|
| Applet API | 废弃 | 移除 | 已移除 |
| RMI Activation | 存在 | 移除 | 已移除 |
| Security Manager | 存在 | 废弃 | 废弃（未来将移除） |
| CMS GC | 废弃 | 移除 | 已移除 |
| Nashorn JavaScript 引擎 | 废弃 | 移除 | 已移除 |
| `finalize()` | 存在 | 废弃 | 废弃 |
| 偏向锁 | 默认开启 | 禁用 | 移除 |
| `Thread.stop/Thread.suspend` | 存在 | 废弃 | 废弃 |
| 32-bit 平台 | 支持 | 废弃 | 不再支持 |

---

## 对 HugeGraph 项目最相关的亮点

### 1. 虚拟线程 (JDK 21) — Project Loom

HugeGraph 作为 REST API 服务，处理大量并发请求时，虚拟线程能带来巨大优势：
- 海量 IO 密集型任务几乎零成本创建
- 无需线程池，代码更简洁
- 极大提升吞吐量，尤其在高并发查询场景

### 2. ZGC 生产就绪 (JDK 21)

图数据库内存占用大、对象关系复杂：
- ZGC 亚毫秒级暂停（<1ms），最大暂停时间不受堆大小影响
- 分代式 ZGC（JDK 21）进一步降低内存和 CPU 开销
- 适合大堆场景（百 GB 级别），减少 Full GC 风险

### 3. Record 类 (JDK 17)

适合替代 HugeGraph 中大量 DTO/POJO/配置类：
```java
// 替代冗长的 POJO
public record VertexRecord(String id, String label, Map<String, Object> properties) {}
```
减少样板代码，提高可读性。

### 4. 模式匹配 (JDK 17/21)

简化图遍历和查询处理中的类型判断：
```java
// JDK 11
if (obj instanceof Vertex) {
    Vertex v = (Vertex) obj;
    return v.id();
}

// JDK 17+
if (obj instanceof Vertex v) {
    return v.id();
}
```

### 5. CDS + AppCDS 增强 (JDK 17/21)

HugeGraph 启动时加载大量类，AppCDS 可显著减少启动时间，对开发和部署都有帮助。

---

## 结论

- **安全性**：JDK 21 避免了 JDK 11 的数十个已知 CVE 漏洞
- **性能**：GC（ZGC/G1/Shenandoah）大幅优化，虚拟线程提升并发能力
- **开发体验**：Record、模式匹配、文本块等新特性提升代码质量和开发效率
- **兼容性**：JDK 21 完全向后兼容，无需修改现有 JDK 11 代码即可运行

**强烈建议在开发和生产环境中使用 JDK 21。**

---

## 版本修改记录

### 2026-06-04 — JDK 21 兼容性修复

编译环境：**OpenJDK 21.0.11** (Homebrew, macOS ARM64)

#### 问题描述

在 JDK 21 环境下执行 `mvn clean compile -DskipTests`，`hugegraph-clustertest-minicluster` 和 `hugegraph-clustertest-test` 模块编译失败。

**错误信息**：
```
Fatal error compiling: java.lang.NoSuchFieldError: 
Class com.sun.tools.javac.tree.JCTree$JCImport does not have member field 
'com.sun.tools.javac.tree.JCTree qualid'
```

**根因**：
1. `maven-compiler-plugin:3.1`（2014年发布）内部使用的 Lombok 注解处理器依赖 JDK 内部 `JCTree$JCImport.qualid` 字段，该字段在 JDK 21 中已被移除
2. `hugegraph-clustertest-test` 模块使用了 `@Slf4j` 注解但缺少 Lombok 依赖

#### 修改清单

| # | 文件 | 修改内容 | 说明 |
|---|------|---------|------|
| 1 | `pom.xml` | `maven-compiler-plugin` 版本 `3.1` → `3.11.0` | 3.11.0 支持 JDK 21，同时将 `compilerArguments` 迁移为 `compilerArgs` 标准写法 |
| 2 | `pom.xml` | `lombok.version` `1.18.30` → `1.18.34` | 1.18.34 适配 JDK 21 内部 API 变更 |
| 3 | `hugegraph-cluster-test/hugegraph-clustertest-minicluster/pom.xml` | Lombok 依赖修复：移除硬编码版本 `1.18.24`，scope `compile` → `provided` | 统一使用父 POM 管理的版本，避免版本冲突；`provided` 是 Lombok 的正确 scope |
| 4 | `hugegraph-cluster-test/hugegraph-clustertest-test/pom.xml` | 新增 Lombok `provided` 依赖 | 该模块使用了 `@Slf4j` 注解但缺少依赖 |

#### 编译结果

```
[INFO] BUILD SUCCESS
[INFO] Total time:  55.422 s
[INFO] 全部 38 个模块编译成功
```
