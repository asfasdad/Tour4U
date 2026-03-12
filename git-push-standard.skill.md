# git-push-standard

## Description

Standardize the post-push report format for all Git push operations.

## Triggers

- 推送
- 上传到git
- 上传到 GitHub
- 发布代码
- 同步远程
- push

## Instructions

When user asks to push/upload code, always execute and report using this standard:

1. Run checks before/after push:
   - `git status --short --branch`
   - `git remote -v`
   - `git branch -vv`
   - `git log -3 --oneline`
2. Perform push safely (no force push unless explicitly requested).
3. Return output strictly in the format below.

## Output Template

```text
推送完成

- 仓库：<owner/repo 或 remote URL>
- 目标远程：<remote 名称>
- 目标分支：<branch 名称>
- 推送结果：<fast-forward / new branch / rejected / 其他>
- 最新提交：<commit hash> <message>
- 本次提交范围：<简述本次改动目的，1 句话>
- 工作区状态：<clean / 有未提交改动>
- 跟踪关系：<local branch...remote branch>

附加说明：
- 如发生拒绝推送，写明原因（例如 non-fast-forward）和处理动作。
- 如发现敏感文件风险（.env / *.pem / *.key / client_secret*.json），必须单独警告。
```

## Guardrails

- Never use `git push --force` unless user explicitly asks.
- Never use `--no-verify` unless user explicitly asks.
- Never modify git config.
- Never skip reporting any required field in template.

## Recovery Rule

If push is rejected by non-fast-forward:

1. `git fetch <remote> <branch>`
2. merge or rebase safely (prefer non-destructive merge if history is unrelated)
3. push again
4. explain what happened in "附加说明"
