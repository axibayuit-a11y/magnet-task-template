# Magnet Task Template

磁力链接转存 OneDrive 的 GitHub Actions 模板仓库。

## 功能

- 通过 GitHub Actions 下载磁力链接
- 自动上传到 OneDrive
- 支持大文件流式上传
- 支持多文件种子

## 使用方式

此仓库由 [ImgBed](https://github.com/MarSevworker/CloudFlare-ImgBed) 自动 Fork 和调用，无需手动操作。

## 策略

- ≤13GB：全部下载后上传
- >13GB 单文件：边下边传（流式）
- >13GB 多文件：逐个下载上传删除
