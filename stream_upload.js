/**
 * stream_upload.js - Magnet Download + OneDrive Upload
 * Version: 1.3
 * 
 * Strategy:
 * - qBittorrent handles magnet metadata and download
 * - Files are uploaded to OneDrive after download
 */

const VERSION = '1.3';

const { spawn, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const CHUNK_SIZE = 30 * 1024 * 1024;
const POLL_INTERVAL = 5000;
const QBT_HOST = process.env.QBT_HOST || 'http://127.0.0.1:8080';
const QBT_USERNAME = process.env.QBT_USERNAME || 'admin';
const QBT_PASSWORD = process.env.QBT_PASSWORD || 'adminadmin';
let qbtCookie = '';

async function main() {
    const magnet = process.env.MAGNET;
    const clientId = process.env.OD_CLIENT_ID;
    const clientSecret = process.env.OD_CLIENT_SECRET;
    const tenantId = process.env.OD_TENANT_ID;
    const refreshToken = process.env.OD_REFRESH_TOKEN;
    const rootPath = process.env.OD_ROOT_PATH || 'imgbed';
    const callbackUrl = process.env.CALLBACK_URL;
    const taskId = process.env.TASK_ID;
    const uploadFolder = process.env.UPLOAD_FOLDER || '';
    const maxTimeHours = parseFloat(process.env.TIMEOUT_HOURS) || 2;
    const stallTimeoutMinutes = 30;
    const trackers = process.env.BT_TRACKERS || '';
    
    // 进度回调 URL（与 callback 同域）
    const progressUrl = callbackUrl ? callbackUrl.replace('/callback', '/progress') : '';

    console.log('=== Magnet Download Task v' + VERSION + ' ===');
    console.log('Magnet:', magnet?.substring(0, 80) + '...');
    console.log('Max Time:', maxTimeHours, 'hours');
    console.log('Stall Timeout:', stallTimeoutMinutes, 'minutes');

    const accessToken = await refreshAccessToken(clientId, clientSecret, tenantId, refreshToken);
    console.log('OneDrive token refreshed');

    const downloadDir = './downloads';
    fs.mkdirSync(downloadDir, { recursive: true });

    const qbt = await startQBittorrent(downloadDir);
    console.log('qBittorrent started');

    console.log('Adding magnet and fetching metadata...');
    const metadata = await downloadMagnetWithQBittorrent(qbt, magnet, trackers, downloadDir, maxTimeHours * 3600000, stallTimeoutMinutes * 60000, createProgressReporter(progressUrl, taskId));
    const totalSize = metadata.totalSize;
    const torrentName = metadata.fileName;
    const fileCount = metadata.fileCount || 1;

    console.log('Name:', torrentName);
    console.log('Size:', (totalSize / 1024 / 1024 / 1024).toFixed(2), 'GB');
    console.log('Files:', fileCount);
    console.log('Upload folder:', uploadFolder || '(root)');
    console.log('Mode: qBittorrent');

    // 构建 OneDrive 上传基础路径：rootPath/uploadFolder/dateFolder
    // 磁力内容会保持原有结构追加在后面
    const now = new Date();
    const dateFolder = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}`;
    
    // 清理 uploadFolder 的前后斜杠
    const cleanUploadFolder = (uploadFolder || '').replace(/^\/+/, '').replace(/\/+$/, '');
    
    // OneDrive 路径：rootPath/uploadFolder/dateFolder/[磁力原有结构]
    const onedrivePath = [rootPath, cleanUploadFolder, dateFolder].filter(p => p).join('/');
    // 图床 KV 路径：uploadFolder/[磁力原有结构]（不含 rootPath 和 dateFolder）
    const kvBasePath = cleanUploadFolder;
    
    console.log('OneDrive base path:', onedrivePath);
    console.log('KV base path:', kvBasePath || '(root)');

    const reportProgress = createProgressReporter(progressUrl, taskId);
    const uploadedFiles = await uploadDownloadedFiles(downloadDir, accessToken, onedrivePath, kvBasePath, reportProgress);

    if (callbackUrl) {
        try {
            await axios.post(callbackUrl, {
                taskId,
                status: 'completed',
                torrentName: torrentName,
                uploadFolder: uploadFolder,
                files: uploadedFiles.map(f => ({
                    fileName: f.name,        // 文件名
                    fileSize: f.size,
                    itemId: f.itemId || '',
                    onedrivePath: f.onedrivePath || '',  // OneDrive 完整路径
                    kvPath: f.kvPath || ''               // 图床 KV 的 fileId
                }))
            });
            console.log('Callback sent');
        } catch (e) {
            console.error('Callback failed:', e.message);
            throw e;
        }
    }

    console.log('All done!');
}

function getAllFiles(dirPath, arr = []) {
    fs.readdirSync(dirPath).forEach(file => {
        const fullPath = path.join(dirPath, file);
        if (fs.statSync(fullPath).isDirectory()) getAllFiles(fullPath, arr);
        else arr.push(fullPath);
    });
    return arr;
}

async function startQBittorrent(downloadDir) {
    const qbtProfileDir = path.resolve('./qbt-profile');
    fs.mkdirSync(qbtProfileDir, { recursive: true });

    // qBittorrent-nox 在 GitHub Actions 里作为本地 Web API 服务运行。
    const qbtArgs = buildQBittorrentArgs(qbtProfileDir);
    let startupLog = '';
    const qbt = spawn('qbittorrent-nox', qbtArgs, {
        env: {
            ...process.env,
            HOME: process.cwd()
        }
    });

    qbt.stdout.on('data', data => {
        const text = data.toString();
        startupLog += text;
        console.log('[qBittorrent]', text);
    });
    qbt.stderr.on('data', data => {
        const text = data.toString();
        startupLog += text;
        console.error('[qBittorrent]', text);
    });

    await waitForQBittorrent();
    await loginQBittorrent(() => startupLog);

    // 统一设置保存目录，避免下载内容散落到默认目录。
    await qbtRequest('POST', '/api/v2/app/setPreferences', new URLSearchParams({
        json: JSON.stringify({
            save_path: path.resolve(downloadDir),
            temp_path_enabled: false,
            autorun_enabled: false,
            start_paused_enabled: false,
            max_active_downloads: -1,
            max_active_torrents: -1
        })
    }), { rawBody: true });

    return qbt;
}

function buildQBittorrentArgs(qbtProfileDir) {
    const args = [
        '--webui-port=8080',
        '--profile=' + qbtProfileDir
    ];

    // 不同 Ubuntu 源里的 qBittorrent 参数不完全一致：
    // 新版才支持 --confirm-legal-notice，旧版带上会直接退出。
    if (isQBittorrentArgSupported('--confirm-legal-notice')) {
        args.unshift('--confirm-legal-notice');
    }

    return args;
}

function isQBittorrentArgSupported(argName) {
    try {
        const result = spawnSync('qbittorrent-nox', ['-h'], {
            encoding: 'utf8',
            timeout: 10000
        });
        const helpText = `${result.stdout || ''}\n${result.stderr || ''}`;
        return helpText.includes(argName);
    } catch (error) {
        console.warn('Unable to inspect qBittorrent options:', error.message);
        return false;
    }
}

async function waitForQBittorrent() {
    const start = Date.now();
    while (Date.now() - start < 30000) {
        try {
            await axios.get(QBT_HOST + '/api/v2/app/version', {
                timeout: 2000,
                validateStatus: () => true
            });
            return;
        } catch (error) {
            await sleep(1000);
        }
    }
    throw new Error('qBittorrent Web API did not start');
}

async function qbtRequest(method, endpoint, data = null, options = {}) {
    const headers = {};
    let body = data;
    if (options.rawBody) {
        headers['Content-Type'] = 'application/x-www-form-urlencoded';
        body = data.toString();
    }
    if (qbtCookie && endpoint !== '/api/v2/auth/login') {
        headers.Cookie = qbtCookie;
    }

    const response = await axios({
        method,
        url: QBT_HOST + endpoint,
        data: body,
        headers,
        timeout: options.timeout || 15000,
        validateStatus: status => status >= 200 && status < 300
    });
    if (endpoint === '/api/v2/auth/login' && response.headers['set-cookie']) {
        qbtCookie = response.headers['set-cookie'].map(cookie => cookie.split(';')[0]).join('; ');
    }
    return response.data;
}

async function loginQBittorrent(getStartupLog) {
    const tempPassword = extractTemporaryPassword(getStartupLog());
    const passwords = [tempPassword, QBT_PASSWORD].filter(Boolean);

    for (const password of passwords) {
        try {
            const result = await qbtRequest('POST', '/api/v2/auth/login', new URLSearchParams({
                username: QBT_USERNAME,
                password
            }), { rawBody: true });
            if (String(result || '').trim() !== 'Ok.') {
                throw new Error('qBittorrent rejected credentials');
            }
            console.log('qBittorrent Web API logged in');
            return;
        } catch (error) {
            console.error('qBittorrent login failed:', error.message);
        }
    }

    throw new Error('qBittorrent Web API login failed');
}

function extractTemporaryPassword(text) {
    const match = String(text || '').match(/temporary password[^:]*:\s*([^\s]+)/i);
    return match ? match[1].trim() : '';
}

async function downloadMagnetWithQBittorrent(qbtProcess, magnet, trackers, downloadDir, maxTime, stallTimeout, reportProgress) {
    const addPayload = new URLSearchParams({
        urls: magnet,
        savepath: path.resolve(downloadDir),
        paused: 'false',
        root_folder: 'true',
        sequentialDownload: 'false',
        firstLastPiecePrio: 'true'
    });

    addPayload.set('tags', 'imgbed');

    await qbtRequest('POST', '/api/v2/torrents/add', addPayload, { rawBody: true });

    const start = Date.now();
    let lastDownloaded = 0;
    let lastProgressTime = Date.now();
    let torrent = null;

    while (Date.now() - start < maxTime) {
        const torrents = await qbtRequest('GET', '/api/v2/torrents/info?tag=imgbed');
        torrent = Array.isArray(torrents) && torrents.length > 0 ? torrents[0] : torrent;
        if (!torrent) {
            const allTorrents = await qbtRequest('GET', '/api/v2/torrents/info');
            torrent = Array.isArray(allTorrents) && allTorrents.length > 0 ? allTorrents[0] : null;
        }

        if (!torrent) {
            await sleep(POLL_INTERVAL);
            continue;
        }

        if (trackers && torrent.hash) {
            await addTrackersOnce(torrent.hash, trackers);
            trackers = '';
        }

        const downloaded = Number(torrent.downloaded || 0);
        const totalSize = Number(torrent.total_size || torrent.size || 0);
        const percent = totalSize > 0 ? Math.floor((downloaded / totalSize) * 100) : Math.floor(Number(torrent.progress || 0) * 100);
        const speed = Number(torrent.dlspeed || 0);

        if (downloaded > lastDownloaded || speed > 0) {
            lastDownloaded = downloaded;
            lastProgressTime = Date.now();
        }

        if (reportProgress) {
            reportProgress({
                phase: 'downloading',
                downloaded: formatBytes(downloaded),
                total: formatBytes(totalSize),
                speed: formatBytes(speed) + '/s',
                eta: formatEta(Number(torrent.eta || 0)),
                percent,
                progress: `${formatBytes(downloaded)}/${formatBytes(totalSize)} (${percent}%) DL:${formatBytes(speed)}/s ETA:${formatEta(Number(torrent.eta || 0))}`
            });
        }

        console.log(`[qBittorrent] ${torrent.name || 'metadata'} ${percent}% ${formatBytes(speed)}/s ${torrent.state || ''}`);

        if (torrent.state === 'error' || torrent.state === 'missingFiles') {
            throw new Error(`qBittorrent download failed: ${torrent.state}`);
        }

        if (torrent.progress >= 1 || torrent.state === 'uploading' || torrent.state === 'stalledUP') {
            await qbtRequest('POST', '/api/v2/torrents/pause', new URLSearchParams({ hashes: torrent.hash }), { rawBody: true }).catch(() => null);
            qbtProcess.kill();
            const files = await getDownloadedFileList(downloadDir);
            return {
                fileName: torrent.name || 'download',
                totalSize: totalSize || files.reduce((sum, file) => sum + file.size, 0),
                fileCount: files.length
            };
        }

        if (Date.now() - lastProgressTime > stallTimeout) {
            throw new Error('No qBittorrent progress for ' + (stallTimeout / 60000) + ' minutes');
        }

        await sleep(POLL_INTERVAL);
    }

    throw new Error('Max time exceeded (' + (maxTime / 3600000) + 'h)');
}

let trackersAdded = false;

async function addTrackersOnce(hash, trackers) {
    if (trackersAdded || !hash || !trackers) return;
    trackersAdded = true;
    const urls = trackers.split(',').filter(Boolean).join('\n');
    if (!urls) return;
    await qbtRequest('POST', '/api/v2/torrents/addTrackers', new URLSearchParams({ hash, urls }), { rawBody: true }).catch(error => {
        console.error('Add trackers failed:', error.message);
    });
}

async function getDownloadedFileList(downloadDir) {
    if (!fs.existsSync(downloadDir)) return [];
    return getAllFiles(downloadDir).map(file => ({
        path: file,
        size: fs.statSync(file).size
    }));
}

async function uploadDownloadedFiles(downloadDir, accessToken, onedrivePath, kvBasePath, reportProgress) {
    console.log('Download complete, uploading...');
    if (reportProgress) {
        reportProgress({ phase: 'uploading', progress: '下载完成，开始上传...', percent: 0 });
    }

    const allFiles = getAllFiles(downloadDir);
    if (allFiles.length === 0) throw new Error('No files found');

    const uploadedFiles = [];

    for (let i = 0; i < allFiles.length; i++) {
        const file = allFiles[i];
        const fileStats = fs.statSync(file);
        const relativePath = path.relative(downloadDir, file).replace(/\\/g, '/');
        const displayFileName = path.basename(relativePath);
        const fileKvPath = kvBasePath ? kvBasePath + '/' + relativePath : relativePath;

        console.log('[' + (i + 1) + '/' + allFiles.length + '] Uploading:', relativePath);

        if (reportProgress) {
            const percent = Math.round((i / allFiles.length) * 100);
            reportProgress({
                phase: 'uploading',
                progress: `上传中 [${i + 1}/${allFiles.length}] ${relativePath}`,
                percent,
                fileIndex: i + 1,
                fileCount: allFiles.length,
                currentFile: relativePath
            });
        }

        const uploadResult = await uploadToOneDrive(file, relativePath, fileStats.size, accessToken, onedrivePath, allFiles.length === 1 ? reportProgress : null);
        uploadedFiles.push({
            name: displayFileName,
            size: fileStats.size,
            itemId: uploadResult.itemId,
            onedrivePath: uploadResult.path,
            kvPath: fileKvPath
        });
    }

    if (reportProgress) {
        reportProgress({ phase: 'completed', progress: `上传完成 ${allFiles.length} 个文件`, percent: 100 });
    }

    return uploadedFiles;
}

async function uploadToOneDrive(filePath, fileName, fileSize, accessToken, basePath, reportProgress = null) {
    const safeName = fileName.replace(/\\/g, '/');
    const fullPath = basePath + '/' + safeName;
    
    if (fileSize <= 4 * 1024 * 1024) {
        // 小文件直接上传
        const response = await axios.put(
            'https://graph.microsoft.com/v1.0/me/drive/root:/' + fullPath + ':/content',
            fs.readFileSync(filePath),
            { headers: { 'Authorization': 'Bearer ' + accessToken } }
        );
        return { itemId: response.data.id, path: fullPath };
    } else {
        // 大文件分片上传
        const session = await createUploadSession(accessToken, basePath, safeName);
        let uploaded = 0;
        let lastResponse = null;
        let lastReportedPercent = 0;
        while (uploaded < fileSize) {
            const end = Math.min(uploaded + CHUNK_SIZE, fileSize);
            lastResponse = await uploadChunk(session.uploadUrl, filePath, uploaded, end, fileSize);
            uploaded = end;
            const percent = Math.round((uploaded / fileSize) * 100);
            if (uploaded % (100 * 1024 * 1024) < CHUNK_SIZE) console.log('Progress:', (uploaded / 1024 / 1024).toFixed(0), 'MB');
            // Report progress (throttling handled by createProgressReporter)
            if (reportProgress) {
                reportProgress({ 
                    phase: 'uploading', 
                    progress: `上传中 ${(uploaded / 1024 / 1024).toFixed(0)}MB / ${(fileSize / 1024 / 1024).toFixed(0)}MB`,
                    percent
                });
            }
        }
        // 最后一个分片的响应包含文件信息
        return { itemId: lastResponse?.id || '', path: fullPath };
    }
}

async function createUploadSession(accessToken, basePath, fileName) {
    const safeName = fileName.replace(/\\/g, '/');
    const response = await axios.post('https://graph.microsoft.com/v1.0/me/drive/root:/' + basePath + '/' + safeName + ':/createUploadSession', { item: { '@microsoft.graph.conflictBehavior': 'rename' } }, { headers: { 'Authorization': 'Bearer ' + accessToken } });
    return response.data;
}

async function uploadChunk(uploadUrl, filePath, start, end, totalSize) {
    const fd = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(end - start);
    fs.readSync(fd, buffer, 0, end - start, start);
    fs.closeSync(fd);
    const response = await axios.put(uploadUrl, buffer, { headers: { 'Content-Length': end - start, 'Content-Range': 'bytes ' + start + '-' + (end - 1) + '/' + totalSize }, maxBodyLength: Infinity, maxContentLength: Infinity });
    return response.data; // 返回响应数据，最后一个分片包含文件信息
}

async function refreshAccessToken(clientId, clientSecret, tenantId, refreshToken) {
    const response = await axios.post('https://login.microsoftonline.com/' + tenantId + '/oauth2/v2.0/token', new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
    return response.data.access_token;
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

// 创建进度报告器 (带并发控制和重试机制)
function createProgressReporter(progressUrl, taskId) {
    let lastReportedPercent = -100;
    let lastPhase = '';
    let isReporting = false;
    let nextReport = null;
    
    // 实际发送请求的函数
    const processQueue = async () => {
        if (isReporting || !nextReport) return;
        
        isReporting = true;
        const data = nextReport;
        nextReport = null; // 清空队列，只处理最新的
        
        try {
            console.log(`Reporting progress (${data.phase}): ${data.percent}%`);
            await axios.post(progressUrl, { taskId, ...data }, { timeout: 10000 }); // 增加超时到10s
            console.log('Progress reported successfully');
            
            // 只有成功才更新标记
            lastReportedPercent = data.percent || 0;
            lastPhase = data.phase;
        } catch (e) {
            console.error('Progress report failed:', e.message);
            // 失败不更新 lastReportedPercent，下次有机会重试
        } finally {
            isReporting = false;
            // 如果在发送期间有新数据进来，继续处理
            if (nextReport) processQueue();
        }
    };

    return (data) => {
        if (!progressUrl || !taskId) return;

        // 阶段变化强制重置
        if (data.phase !== lastPhase && lastPhase !== '') {
            lastReportedPercent = -100;
        }

        const currentPercent = data.percent || 0;
        const isUpload = data.phase && data.phase.includes('upload');
        const threshold = isUpload ? 35 : 10;
        
        // 关键事件强制上报：完成、元数据、或者达到阈值
        const isImportant = 
            data.phase === 'completed' || 
            data.phase === 'metadata' ||
            data.phase !== lastPhase || 
            (currentPercent - lastReportedPercent >= threshold);

        if (isImportant) {
            nextReport = data;
            processQueue();
        }
    };
}

function formatBytes(bytes) {
    const value = Number(bytes || 0);
    if (!Number.isFinite(value) || value <= 0) return '0B';

    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = value;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }

    return `${size.toFixed(unitIndex === 0 ? 0 : 1)}${units[unitIndex]}`;
}

function formatEta(seconds) {
    const value = Number(seconds || 0);
    if (!Number.isFinite(value) || value < 0 || value >= 8640000) return '--';

    const hours = Math.floor(value / 3600);
    const minutes = Math.floor((value % 3600) / 60);
    const secs = Math.floor(value % 60);
    if (hours > 0) return `${hours}h${minutes}m`;
    if (minutes > 0) return `${minutes}m${secs}s`;
    return `${secs}s`;
}

main().catch(err => { console.error('Fatal error:', err); process.exit(1); });
