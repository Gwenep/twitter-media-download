# Twitter Media Downloader

[![Go Report Card](https://goreportcard.com/badge/github.com/Gwenep/twitter-media-download)](https://goreportcard.com/report/github.com/Gwenep/twitter-media-download)
[![Go](https://github.com/Gwenep/twitter-media-download/actions/workflows/go.yml/badge.svg)](https://github.com/Gwenep/twitter-media-download/actions/workflows/go.yml)
![GitHub Release](https://img.shields.io/github/v/release/Gwenep/twitter-media-download) 
![GitHub License](https://img.shields.io/github/license/Gwenep/twitter-media-download?logo=github)

跨平台的推特媒体下载器。用于轻松，快速，安全，整洁，批量的下载推特上用户的推文。支持手动指定用户或通过列表、用户关注批量下载。开箱即用！

## Feature

- 下载指定用户的媒体推文 (video, img, gif)
- 保留推文标题
- 保留推文发布日期，设置为文件的修改时间
- 以列表为单位批量下载
- 关注中的用户批量下载
- 在文件系统中保留列表/关注结构
- 同步用户/列表信息：名称，是否受保护，等。。。
- 记录用户曾用名
- 避免重复下载
  - 每次工作后记录用户的最新发布时间，下次工作仅从这个时间点开始拉取用户推文
  - 通过 `media_files` 表精确记录 “推文 + 媒体地址 -> 文件名”，重试时不会产生重复文件
  - 向列表目录发送指向用户目录的符号链接，无论多少列表包含同一用户，本地仅保存一份用户存档
- 避免重复获取时间线：只有下载成功的推文才会推进“最新发布时间”。下载失败的推文不会被跳过，下次运行仍会重新拉取并重试（失败列表同时保存在本地，可在退出前自动重试）
- 避免重复同步用户（更新用户信息，获取时间线，下载推文）
- 速率限制：避免触发 Twitter API 速率限制
- 自动关注受保护的用户
- 添加备用 cookie：提高推文获取速度和总数量

## How to use

### 下载/编译

**直接下载**

前往 [Release](https://github.com/Gwenep/twitter-media-download/releases/latest) 自行选择合适的版本并下载

**自行编译**

```bash
git clone https://github.com/Gwenep/twitter-media-download.git
cd twitter-media-download
go build .
```

> 数据库驱动使用纯 Go 实现的 `modernc.org/sqlite`，不再依赖 CGO，也不需要安装 gcc / MinGW / Visual Studio。
> Windows 上可以直接 `go build -o tmd.exe .`，也可以加 `CGO_ENABLED=0` 交叉编译其它平台，例如：
>
> ```bash
> CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o tmd-linux-amd64 .
> ```

**在 GitHub 上编译（无需本地安装编译器）**

1. Fork 本仓库，把修改推送到自己的仓库；
2. 打开仓库的 `Actions` -> `Go CI/CD` -> `Run workflow`（也可以直接推送 `master` 分支触发）；
3. 运行完成后在该次运行页面底部的 `Artifacts` 中下载对应平台的构建产物（Windows 为 `tmd-Windows`，解压后即 `tmd-Windows-amd64.exe`）。

打 `v*` 标签推送时，工作流会照旧把产物上传到 GitHub Release。

### 更新/填写配置

第一次运行程序时，程序会询问如下配置信息，请按要求将配置项依次填入

#### 配置项介绍

1. `storeage path`：存储路径(可以不存在)
2. `auth_token`：用于登录，[获取方式](https://github.com/Gwenep/twitter-media-download/blob/master/doc/help.md#获取-cookie)
3. `ct0`：用于登录，[获取方式](https://github.com/Gwenep/twitter-media-download/blob/master/doc/help.md#获取-cookie)
4. `max_download_routine`：最大并发下载协程数（如果为0取默认值）

#### 更新配置

```shell
tmd --conf
```

> **执行上述命令将导致引导配置程序重新运行，这将重新配置整个配置文件，而不是单独的配置项。单独修改配置项**请至 `%appdata%/.tmd2/conf.yaml` 或 `$HOME/.tmd2/conf.yaml`手动修改

### 命令说明

```
tmd --help                 // 显示帮助
tmd --conf                 // 重新运行配置程序
tmd --user <user_id>       // 下载由 user_id 指定的用户的推文
tmd --user <screen_name>   // 下载由 screen_name 指定的用户的推文
tmd --list <list_id>       // 批量下载由 list_id 指定的列表中的每个用户
tmd --foll <user_id>       // 批量下载由 user_id 指定的用户正关注的每个用户
tmd --foll <screen_name>   // 批量下载由 screen_name 指定的用户正关注的每个用户
tmd --auto-follow          // 自动关注受保护的用户
tmd --no-retry             // 仅转储，不在程序退出前自动重试下载失败的推文
```

> 为了创建符号链接，在 Windows 上应该以管理员身份运行程序

[不知道啥是 user_id/list_id/screen_name?](https://github.com/Gwenep/twitter-media-download/blob/master/doc/help.md#%E8%8E%B7%E5%8F%96-list_id-user_id-screen_name)

### 示例

```
tmd --user elonmusk  // 下载 screen_name 为 ‘eronmusk’ 的用户
tmd --user 1234567   // 下载 user_id 为 1234567 的用户
tmd --list 8901234   // 下载 list_id 为 8901234 的列表
tmd --foll 567890    // 下载 user_id 为 567890 的用户正关注的所有用户
```

更推荐的做法：一次运行

```shell
tmd --user elonmusk --user 1234567 --list 8901234 --foll 567890
```

### 设置代理

运行前通过环境变量指定代理服务器（TUN 模式跳过这一步）

```bash
set HTTP_PROXY=url
set HTTPS_PROXY=url
```

示例：
```bash
set HTTP_PROXY=http://127.0.0.1:7890
set HTTPS_PROXY=http://127.0.0.1:7890
tmd --user elonmusk
```

如果你使用windows系统，在powershell中使用以下指令设置代理：
```powershell
$Env:HTTP_PROXY="http://127.0.0.1:7890"
$Env:HTTPS_PROXY="http://127.0.0.1:7890"
```

### 忽略用户

程序默认会忽略被静音或被屏蔽的用户，所以当你想要下载的列表中包含你不想包含的用户，可以在推特将他们屏蔽或静音

### 添加额外 cookie

程序动态从所有可用 cookie 中选择一个不会被速率限制的 cookie 请求用户推文，以避免因单一 cookie 的速率限制导致程序被阻塞。

按如下格式创建 `$HOME/.tmd2/additional_cookies.yaml` 或 `%appdata%/.tmd2/additional_cookies.yaml`

```yaml
- auth_token: xxxxxxxxx1
  ct0: xxxxxxxxxxxxxxxxxxxxxxx
- auth_token: xxxxxxxxx2
  ct0: xxxxxxxxxxxxxxxx2
- auth_token: xxxxxxxxxxxxxxxx3
  ct0: xxxxxxxxxxxxxxxxxxxxx3
```
> 这些添加的备用 cookie，仅用来提升获取推文的速率和总量。判断是否忽略用户和自动关注受保护的用户依然使用主账号

## Detail

### 存储目录迁移与旧数据兼容

每个用户/列表文件夹内都会写入一个身份标记文件：

- 用户目录：`.tmd_user.json`（记录 `user_id`、昵称、用户名）
- 列表目录：`.tmd_list.json`（记录 `list_id`、列表名）

数据库中的路径是绝对路径。重装系统、更换盘符或移动整个存储目录后，程序会通过标记文件把数据库记录重新关联到移动后的文件夹：

- 已经下载过的内容不会被重复下载，也不会因为根目录变化而重新拉取全部历史
- 每个用户的媒体始终保存在自己的文件夹中，不会因为同名/路径失效而全部堆到一个文件夹
- 旧版本创建的文件夹没有标记文件时，只有在“数据库记录中的文件夹名确实存在”时才会被认领；明确属于其他用户的文件夹绝不会被复用
- 旧版本下载的媒体文件会按“文件修改时间 == 推文发布时间”识别，升级后第一次运行不会重复下载

> 如果旧版本已经把多个用户的内容写进了同一个文件夹，升级不会删除任何已有文件；程序会为每个用户重新关联/建立属于自己的文件夹，缺失的内容会在需要时补下。

### 下载失败与进度推进

“最新发布时间”只会在下载结束后推进：

- 全部成功时，推进到本次最新推文
- 有条目失败时，只推进到“比最旧失败推文更旧的最新一条成功推文”，保证失败的推文下一次仍在拉取范围内
- 最旧推文失败或任务被中断时不推进，宁可下次多拉取一些，也不会永久漏掉内容
- 单条媒体返回 403/404/410（被删除、DMCA 等）时只跳过该媒体，同一条推文的其它媒体仍会继续下载，也不会导致无限重试

### 关于速率限制

Twitter API 限制一段时间内过快的请求 （例如某端点每15分钟仅允许请求500次，超出这个次数会以429响应），当某一端点将要达到速率限制程序会打印一条通知并阻塞尝试请求这个端点的协程直到余量刷新（这最多是15分钟），但并不会阻塞所有协程，所以其余协程打印的消息可能将这条休眠通知覆盖让人认为程序无响应了，等待余量刷新程序会继续工作。

## Contributors

![](https://contrib.rocks/image?repo=Gwenep/twitter-media-download)

