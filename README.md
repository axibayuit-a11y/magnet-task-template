# Magnet Task

磁力链接下载并上传到 OneDrive 的自动化任务。

## 使用方式

通过 API 触发 `api_trigger_magnet` 事件，传入以下环境变量：

- `MAGNET`: 磁力链接
- `OD_REFRESH_TOKEN`: OneDrive Refresh Token
- `OD_CLIENT_ID`: OneDrive Client ID
- `OD_CLIENT_SECRET`: OneDrive Client Secret
- `OD_TENANT_ID`: OneDrive Tenant ID
- `TARGET_FOLDER`: 目标文件夹（可选，默认 downloads）
- `CALLBACK_URL`: 完成回调 URL（可选）
- `TASK_ID`: 任务 ID（可选）
