# Skill: git-push-standard

## Purpose

统一每次 Git 推送后的回执格式，确保团队成员都能快速确认：推送到哪里、推了什么、当前是否干净、是否可追溯。

## When To Use

- 用户提到：`推送`、`上传到 git`、`发布代码`、`同步远程`
- 执行完 `git push` 后需要给出标准化结果

## Required Checks

1. `git status --short --branch`
2. `git remote -v`
3. `git branch -vv`
4. `git log -3 --oneline`

## Output Standard (Must Follow)

按下面模板输出，不要省略字段：

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
- 如发生拒绝推送，写明原因（例如 non-fast-forward）和已采取动作（如 fetch/merge/rebase）。
- 若发现潜在敏感文件（如 .env、密钥、凭据），必须单独警告。
```

## Rejection Handling Standard

遇到 `non-fast-forward` 时，优先策略：

1. `git fetch <remote> <branch>`
2. 合并远程历史（避免强推）
3. 再次 `git push`
4. 在回执中说明处理路径与结果

## Security Guardrails

- 禁止 `--force`（除非用户明确要求）
- 禁止 `--no-verify`
- 禁止绕过凭据或修改全局 git config
- 推送前检查是否包含凭据文件：`.env`、`*.pem`、`*.key`、`client_secret*.json`

## Quick Response Example

```text
推送完成

- 仓库：https://github.com/asfasdad/Tour4U.git
- 目标远程：origin
- 目标分支：master
- 推送结果：fast-forward
- 最新提交：46220d9 docs: add current version notes
- 本次提交范围：补充 README 的当前版本说明，统一版本记录方式。
- 工作区状态：clean
- 跟踪关系：master...origin/master
```
