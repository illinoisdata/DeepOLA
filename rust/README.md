# Install dev tools

Compiler: https://doc.rust-lang.org/cargo/getting-started/installation.html

VS Code (IDE): https://code.visualstudio.com/


# Build

Go to the `runtime` folder, then type:
```
cargo test
```

If you want to see logs printed, run
```
sh test_with_log.sh
```

# Running Examples

```
cargo run --example hello
```


# Plan

1. `runtime`: bare-minimum framework for multithread processing of a DAG-structured execution plan.
2. `opt`: optimizes the DAG to an alternative structure for more efficient processing (e.g., predicate pushdown).
3. `csv`: high-level language for processing array-like data (e.g., `awk`); this project will translate text to DAG.
4. `json`: high-level language for processing json-like semi-structured data (e.g., `jq`); this project will translate text to DAG.
