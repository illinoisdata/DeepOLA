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

Run benchmark:
```
cargo bench
```

# Running Examples

```
cargo run --example hello
```



# PR Rules

## Start from main

Before starting your task, create a new branch from `main`
```
(in main) git checkout -b netid/issue-no
(now in netid/issue-no branch)
```

Make commits in this new branch and push to origin for a new PR.


## Rebase on main

If there were new changes in the `main` branch after you create a new branch, "rebase" on the main branch
before creating a PR:
```
git rebase main
```
Before rebasing, it might be helpful to squashing your micro commits into one.

If there were new changes in the `main` branch after you already created a PR, you can do the following:
```
git rebase main
git push -f
```
This will create your PR on top of the current `main`.



## Squash micro commits


If your PR may include multiple commits, consider squashing them into a single commit by running
```
git rebase -i (commit-id-of-main)
```

You can check a proper commit id by
```
git log -20
```
and see which commit id is pointing to `origin/main`.



# Plan

1. `runtime`: bare-minimum framework for multithread processing of a DAG-structured execution plan.
2. `opt`: optimizes the DAG to an alternative structure for more efficient processing (e.g., predicate pushdown).
3. `csv`: high-level language for processing array-like data (e.g., `awk`); this project will translate text to DAG.
4. `json`: high-level language for processing json-like semi-structured data (e.g., `jq`); this project will translate text to DAG.
