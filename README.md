# Magnet Task Template

磁力链接转存 OneDrive 的 GitHub Actions 模板仓库。

## 功能

- 通过 GitHub Actions 下载磁力链接
- 使用 qBittorrent 下载磁力链接
- 自动上传到 OneDrive
- 支持多文件种子

## 使用方式

此仓库由 [ImgBed](https://github.com/MarSevworker/CloudFlare-ImgBed) 自动 Fork 和调用，无需手动操作。

## 策略

- qBittorrent 负责获取磁力元数据和下载文件
- 下载完成后按原始文件结构上传到 OneDrive
- 回调 ImgBed 写入图床索引
