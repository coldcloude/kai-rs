# kai-codegen 代码生成工具
命令行工具，将Rust代码中的struct定义转换为TypeScript type定义

## 功能
- 解析Rust源代码文件，提取struct定义
- 将Rust struct转换为TypeScript type定义
- 支持基本类型转换（i32/i64/u32/u64/f32/f64/bool/String等）
- 支持嵌套struct和Option类型
- 支持Vec/Array类型
- 生成TypeScript代码并写入.ts文件

## 使用方式

```bash
# 编译工具
cd kai-codegen
cargo build --release

# 使用工具转换
kai-codegen <input.rs> <output.ts>

# 或者使用cargo run
cargo run -- <input.rs> <output.ts>
```

## 实现约定

### 类型映射规则
- Rust基本类型 → TypeScript基本类型
  - i8/i16/i32/i64/isize → number
  - u8/u16/u32/u64/usize → number
  - f32/f64 → number
  - bool → boolean
  - String/&str → string
  - char → string
- Option\<T\> → T | null
- Vec\<T\> → T[]
- Option\<Vec\<T\>\> → T[] | null
- 自定义struct类型保持原名

### 输入输出
- 输入：.rs文件路径
- 输出：.ts文件路径
- 支持单个文件转换
