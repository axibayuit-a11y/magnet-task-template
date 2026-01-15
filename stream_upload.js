/**
 * stream_upload.js - Magnet Download + OneDrive Upload
 * Strategy:
 * - <=13GB: Download all then upload
 * - >13GB single file: Stream upload while downloading
 * - >13GB multi-file: Download one, upload one, delete one
 */

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const CHUNK_SIZE = 30 * 1024 * 1024;
const LARGE_FILE_THRESHOLD = 13 * 1024 * 1024 * 1024;
const POLL_INTERVAL = 5000;

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

    console.log('=== Magnet Download Task ===');
    console.log('Magnet:', magnet?.substring(0, 80) + '...');
    console.log('Max Time:', maxTimeHours, 'hours');
    console.log('Stall Timeout:', stallTimeoutMinutes, 'minutes');

    const accessToken = await refreshAccessToken(clientId, clientSecret, tenantId, refreshToken);
    console.log('OneDrive token refreshed');

    const downloadDir = './downloads';
    fs.mkdirSync(downloadDir, { recursive: true });

    console.log('Fetching metadata...');
    const metadata = await fetchMetadata(magnet, trackers, downloadDir);
    const totalSize = metadata.totalSize;
    const torrentName = metadata.fileName;
    const fileCount = metadata.fileCount || 1;
    const fileList = metadata.fileList || [];

    console.log('Name:', torrentName);
    console.log('Size:', (totalSize / 1024 / 1024 / 1024).toFixed(2), 'GB');
    console.log('Files:', fileCount);
    console.log('Upload folder:', uploadFolder || '(root)');

    const isLarge = totalSize > LARGE_FILE_THRESHOLD;
    const isMultiFile = fileCount > 1;
    
    let mode = !isLarge ? 'normal' : (isMultiFile ? 'sequential' : 'streaming');
    console.log('Mode:', mode);

    // 构建上传基础路径：rootPath/uploadFolder/dateFolder/torrentName
    const now = new Date();
    const dateFolder = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}`;
    const uploadBasePath = [rootPath, uploadFolder, dateFolder, torrentName].filter(p => p).join('/');
    console.log('Upload path:', uploadBasePath);

    // 进度报告函数
    const reportProgress = createProgressReporter(progressUrl, taskId);

    let uploadedFiles = [];
    const maxTime = maxTimeHours * 3600000;
    const stallTimeout = stallTimeoutMinutes * 60000;
    
    if (mode === 'normal') {
        uploadedFiles = await normalDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, uploadBasePath, maxTime, stallTimeout, reportProgress);
    } else if (mode === 'streaming') {
        uploadedFiles = await streamingDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, uploadBasePath, maxTime, stallTimeout, reportProgress);
    } else {
        uploadedFiles = await sequentialDownloadAndUpload(magnet, trackers, downloadDir, torrentName, fileList, fileCount, accessToken, uploadBasePath, maxTime, stallTimeout, reportProgress);
    }

    if (callbackUrl) {
        try {
            await axios.post(callbackUrl, {
                taskId,
                status: 'completed',
                torrentName: torrentName,
                files: uploadedFiles.map(f => ({
                    fileName: f.name,
                    fileSize: f.size,
                    itemId: f.itemId || '',
                    path: f.path || ''
                }))
            });
            console.log('Callback sent');
        } catch (e) {
            console.error('Callback failed:', e.message);
        }
    }

    console.log('All done!');
}

async function fetchMetadata(magnet, trackers, downloadDir) {
    return new Promise((resolve, reject) => {
        const args = [magnet, '--dir=' + downloadDir, '--bt-metadata-only=true', '--bt-save-metadata=true', '--file-allocation=none', '--seed-time=0'];
        if (trackers) args.push('--bt-tracker=' + trackers);

        const aria2 = spawn('aria2c', args);
        let fileName = '', totalSize = 0, fileCount = 1, fileList = [], output = '';

        aria2.stdout.on('data', (data) => {
            const str = data.toString();
            output += str;
            console.log(str);
            const nameMatch = output.match(/Name: (.+)/);
            const sizeMatch = output.match(/Total Length: ([\d,]+)/);
            if (nameMatch) fileName = nameMatch[1].trim();
            if (sizeMatch) totalSize = parseInt(sizeMatch[1].replace(/,/g, ''));
            const filesMatch = str.match(/(\d+) files/i);
            if (filesMatch) fileCount = parseInt(filesMatch[1]);
        });

        aria2.stderr.on('data', (data) => console.error(data.toString()));

        aria2.on('close', () => {
            const torrentFiles = fs.readdirSync(downloadDir).filter(f => f.endsWith('.torrent'));
            if (torrentFiles.length > 0) {
                try {
                    const torrentPath = path.join(downloadDir, torrentFiles[0]);
                    const showFilesOutput = execSync('aria2c --show-files "' + torrentPath + '"', { encoding: 'utf8', timeout: 30000 });
                    const lines = showFilesOutput.split('\n');
                    for (const line of lines) {
                        const indexMatch = line.match(/^(\d+)\|/);
                        const pathMatch = line.match(/path=([^|]+)/);
                        const sizeMatch = line.match(/length=([\d]+)/);
                        if (indexMatch && pathMatch) {
                            fileList.push({ index: parseInt(indexMatch[1]), path: pathMatch[1], size: sizeMatch ? parseInt(sizeMatch[1]) : 0 });
                        }
                    }
                    if (fileList.length > 0) fileCount = fileList.length;
                } catch (e) { console.error('Parse error:', e.message); }
            }
            
            if (fileName && totalSize > 0) resolve({ fileName, totalSize, fileCount, fileList });
            else if (torrentFiles.length > 0) resolve({ fileName: torrentFiles[0].replace('.torrent', ''), totalSize: 1024 * 1024 * 1024, fileCount, fileList });
            else reject(new Error('Failed to get metadata'));
        });

        setTimeout(() => { aria2.kill(); resolve({ fileName: 'download', totalSize: 1024 * 1024 * 1024, fileCount: 1, fileList: [] }); }, 120000);
    });
}

function getAllFiles(dirPath, arr = []) {
    fs.readdirSync(dirPath).forEach(file => {
        const fullPath = path.join(dirPath, file);
        if (fs.statSync(fullPath).isDirectory()) getAllFiles(fullPath, arr);
        else if (!file.endsWith('.torrent') && !file.endsWith('.aria2')) arr.push(fullPath);
    });
    return arr;
}

async function normalDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, rootPath, maxTime, stallTimeout, reportProgress) {
    console.log('[Normal] Starting download...');
    const args = [magnet, '--dir=' + downloadDir, '--file-allocation=none', '--seed-time=0', '--max-connection-per-server=16', '--bt-max-peers=150', '--summary-interval=10'];
    if (trackers) args.push('--bt-tracker=' + trackers);

    await new Promise((resolve, reject) => {
        const aria2 = spawn('aria2c', args);
        let lastProgress = 0;
        let lastProgressTime = Date.now();
        
        const maxTimer = setTimeout(() => { 
            aria2.kill(); 
            reject(new Error('Max time exceeded (' + (maxTime / 3600000) + 'h)')); 
        }, maxTime);
        
        const stallChecker = setInterval(() => {
            if (Date.now() - lastProgressTime > stallTimeout) {
                clearInterval(stallChecker);
                clearTimeout(maxTimer);
                aria2.kill();
                reject(new Error('No progress for ' + (stallTimeout / 60000) + ' minutes'));
            }
        }, 30000);
        
        aria2.stdout.on('data', (data) => {
            const str = data.toString();
            console.log(str);
            const cnMatch = str.match(/CN:(\d+)/);
            const progressMatch = str.match(/(\d+)%/);
            if (cnMatch && parseInt(cnMatch[1]) > 0) lastProgressTime = Date.now();
            if (progressMatch) {
                const progress = parseInt(progressMatch[1]);
                if (progress > lastProgress) {
                    lastProgress = progress;
                    lastProgressTime = Date.now();
                }
            }
            // 报告进度
            const progressInfo = parseAria2Progress(str);
            if (progressInfo && reportProgress) {
                reportProgress({ ...progressInfo, phase: 'downloading' });
            }
        });
        aria2.stderr.on('data', (data) => console.error(data.toString()));
        aria2.on('close', (code) => { 
            clearTimeout(maxTimer); 
            clearInterval(stallChecker);
            code === 0 ? resolve() : reject(new Error('Download failed')); 
        });
    });

    console.log('Download complete, uploading...');
    const items = fs.readdirSync(downloadDir).filter(f => !f.endsWith('.torrent') && !f.endsWith('.aria2'));
    if (items.length === 0) throw new Error('No files found');
    
    const firstItem = path.join(downloadDir, items[0]);
    const stats = fs.statSync(firstItem);
    const uploadedFiles = [];
    
    if (stats.isDirectory()) {
        const allFiles = getAllFiles(firstItem);
        console.log('Multi-file:', allFiles.length, 'files');
        for (let i = 0; i < allFiles.length; i++) {
            const file = allFiles[i];
            const fileStats = fs.statSync(file);
            const relativePath = path.relative(firstItem, file);
            const onedrivePath = items[0] + '/' + relativePath.replace(/\\/g, '/');
            console.log('[' + (i + 1) + '/' + allFiles.length + '] Uploading:', relativePath);
            await uploadToOneDrive(file, onedrivePath, fileStats.size, accessToken, rootPath);
            uploadedFiles.push({ name: relativePath, size: fileStats.size });
        }
    } else {
        await uploadToOneDrive(firstItem, items[0], stats.size, accessToken, rootPath);
        uploadedFiles.push({ name: items[0], size: stats.size });
    }
    return uploadedFiles;
}

async function streamingDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, rootPath, maxTime, stallTimeout, reportProgress) {
    console.log('[Streaming] Starting download...');
    const args = [magnet, '--dir=' + downloadDir, '--stream-piece-selector=inorder', '--bt-prioritize-piece=head', '--file-allocation=none', '--seed-time=0', '--max-connection-per-server=16', '--bt-max-peers=150', '--summary-interval=10'];
    if (trackers) args.push('--bt-tracker=' + trackers);

    const aria2 = spawn('aria2c', args);
    let lastProgressTime = Date.now();
    
    aria2.stdout.on('data', (data) => {
        const str = data.toString();
        console.log(str);
        const cnMatch = str.match(/CN:(\d+)/);
        const progressMatch = str.match(/(\d+)%/);
        if ((cnMatch && parseInt(cnMatch[1]) > 0) || progressMatch) {
            lastProgressTime = Date.now();
        }
        // 报告进度
        const progressInfo = parseAria2Progress(str);
        if (progressInfo && reportProgress) {
            reportProgress({ ...progressInfo, phase: 'streaming' });
        }
    });
    aria2.stderr.on('data', (data) => console.error(data.toString()));

    await sleep(5000);

    let actualFileName = '';
    for (let i = 0; i < 60; i++) {
        const items = fs.readdirSync(downloadDir).filter(f => !f.endsWith('.torrent') && !f.endsWith('.aria2'));
        if (items.length > 0 && fs.statSync(path.join(downloadDir, items[0])).isFile()) {
            actualFileName = items[0];
            break;
        }
        await sleep(1000);
    }
    if (!actualFileName) throw new Error('File not found');

    const uploadSession = await createUploadSession(accessToken, rootPath, actualFileName);
    const uploadUrl = uploadSession.uploadUrl;
    console.log('Upload session created');

    let uploadedBytes = 0;
    const uploadInterval = totalSize * 0.1;

    const pollLoop = setInterval(async () => {
        try {
            const actualFile = path.join(downloadDir, actualFileName);
            if (!fs.existsSync(actualFile)) return;
            const downloadedBytes = fs.statSync(actualFile).size;
            if (downloadedBytes - uploadedBytes >= uploadInterval || downloadedBytes >= totalSize) {
                const uploadEnd = Math.min(downloadedBytes, totalSize);
                while (uploadedBytes + CHUNK_SIZE <= uploadEnd) {
                    const chunkEnd = Math.min(uploadedBytes + CHUNK_SIZE, uploadEnd);
                    await uploadChunk(uploadUrl, actualFile, uploadedBytes, chunkEnd, totalSize);
                    uploadedBytes = chunkEnd;
                    console.log('Uploaded:', (uploadedBytes / 1024 / 1024).toFixed(0), 'MB');
                }
            }
        } catch (e) { console.error('Poll error:', e.message); }
    }, POLL_INTERVAL);

    const stallChecker = setInterval(() => {
        if (Date.now() - lastProgressTime > stallTimeout) {
            clearInterval(stallChecker);
            clearInterval(pollLoop);
            aria2.kill();
        }
    }, 30000);

    await new Promise((resolve, reject) => {
        const maxTimer = setTimeout(() => { 
            aria2.kill(); 
            clearInterval(pollLoop); 
            clearInterval(stallChecker);
            reject(new Error('Max time exceeded')); 
        }, maxTime);
        
        aria2.on('close', (code) => { 
            clearTimeout(maxTimer); 
            clearInterval(pollLoop); 
            clearInterval(stallChecker);
            if (Date.now() - lastProgressTime > stallTimeout) {
                reject(new Error('No progress timeout'));
            } else {
                resolve();
            }
        });
    });

    const actualFile = path.join(downloadDir, actualFileName);
    const finalSize = fs.statSync(actualFile).size;
    while (uploadedBytes < finalSize) {
        const chunkEnd = Math.min(uploadedBytes + CHUNK_SIZE, finalSize);
        await uploadChunk(uploadUrl, actualFile, uploadedBytes, chunkEnd, finalSize);
        uploadedBytes = chunkEnd;
    }
    console.log('Upload complete!');
    return [{ name: actualFileName, size: finalSize }];
}

async function sequentialDownloadAndUpload(magnet, trackers, downloadDir, torrentName, fileList, fileCount, accessToken, rootPath, maxTime, stallTimeout, reportProgress) {
    console.log('[Sequential] File-by-file download...');
    const uploadedFiles = [];
    const startTime = Date.now();
    
    if (fileList.length === 0) {
        console.log('Getting file list...');
        const metaArgs = [magnet, '--dir=' + downloadDir, '--bt-metadata-only=true', '--bt-save-metadata=true', '--seed-time=0'];
        if (trackers) metaArgs.push('--bt-tracker=' + trackers);
        
        await new Promise((resolve) => {
            const aria2 = spawn('aria2c', metaArgs);
            aria2.stdout.on('data', (data) => console.log(data.toString()));
            aria2.stderr.on('data', (data) => console.error(data.toString()));
            aria2.on('close', () => resolve());
            setTimeout(() => { aria2.kill(); resolve(); }, 60000);
        });
        
        const torrentFiles = fs.readdirSync(downloadDir).filter(f => f.endsWith('.torrent'));
        if (torrentFiles.length === 0) throw new Error('No torrent file');
        
        try {
            const showFilesOutput = execSync('aria2c --show-files "' + path.join(downloadDir, torrentFiles[0]) + '"', { encoding: 'utf8', timeout: 30000 });
            for (const line of showFilesOutput.split('\n')) {
                const indexMatch = line.match(/^(\d+)\|/);
                const pathMatch = line.match(/path=([^|]+)/);
                const sizeMatch = line.match(/length=([\d]+)/);
                if (indexMatch && pathMatch) fileList.push({ index: parseInt(indexMatch[1]), path: pathMatch[1], size: sizeMatch ? parseInt(sizeMatch[1]) : 0 });
            }
            fileCount = fileList.length;
        } catch (e) {
            console.error('Failed to get file list:', e.message);
            return normalDownloadAndUpload(magnet, trackers, downloadDir, 0, accessToken, rootPath, maxTime, stallTimeout);
        }
    }
    
    for (let i = 0; i < fileList.length; i++) {
        if (Date.now() - startTime > maxTime) {
            throw new Error('Max time exceeded');
        }
        
        const fileInfo = fileList[i];
        console.log('[' + (i + 1) + '/' + fileCount + '] Downloading file', fileInfo.index);
        
        // Clean up
        fs.readdirSync(downloadDir).filter(f => !f.endsWith('.torrent')).forEach(f => {
            const fp = path.join(downloadDir, f);
            fs.statSync(fp).isDirectory() ? fs.rmSync(fp, { recursive: true, force: true }) : fs.unlinkSync(fp);
        });
        
        const args = [magnet, '--dir=' + downloadDir, '--select-file=' + fileInfo.index, '--file-allocation=none', '--seed-time=0', '--max-connection-per-server=16', '--bt-max-peers=150', '--summary-interval=10'];
        if (trackers) args.push('--bt-tracker=' + trackers);

        await new Promise((resolve, reject) => {
            const aria2 = spawn('aria2c', args);
            let lastProgressTime = Date.now();
            
            const remainingTime = maxTime - (Date.now() - startTime);
            const perFileTime = Math.max(remainingTime / (fileCount - i) * 2, stallTimeout * 2);
            
            const maxTimer = setTimeout(() => { 
                aria2.kill(); 
                reject(new Error('File timeout')); 
            }, perFileTime);
            
            const stallChecker = setInterval(() => {
                if (Date.now() - lastProgressTime > stallTimeout) {
                    clearInterval(stallChecker);
                    clearTimeout(maxTimer);
                    aria2.kill();
                    reject(new Error('No progress for file ' + fileInfo.index));
                }
            }, 30000);
            
            aria2.stdout.on('data', (data) => {
                const str = data.toString();
                console.log(str);
                const cnMatch = str.match(/CN:(\d+)/);
                if (cnMatch && parseInt(cnMatch[1]) > 0) lastProgressTime = Date.now();
                // 报告进度
                const progressInfo = parseAria2Progress(str);
                if (progressInfo && reportProgress) {
                    reportProgress({ ...progressInfo, phase: 'sequential', fileIndex: i + 1, fileCount });
                }
            });
            aria2.stderr.on('data', (data) => console.error(data.toString()));
            aria2.on('close', (code) => { 
                clearTimeout(maxTimer); 
                clearInterval(stallChecker);
                code === 0 ? resolve() : reject(new Error('Download failed')); 
            });
        });

        const downloadedItems = fs.readdirSync(downloadDir).filter(f => !f.endsWith('.torrent') && !f.endsWith('.aria2'));
        if (downloadedItems.length === 0) { console.error('File not found'); continue; }
        
        let actualFilePath = '';
        const firstItem = path.join(downloadDir, downloadedItems[0]);
        if (fs.statSync(firstItem).isDirectory()) {
            const allFiles = getAllFiles(firstItem);
            if (allFiles.length > 0) actualFilePath = allFiles[0];
        } else {
            actualFilePath = firstItem;
        }
        
        if (!actualFilePath || !fs.existsSync(actualFilePath)) { console.error('Cannot find file'); continue; }
        
        const actualFileStats = fs.statSync(actualFilePath);
        const relativePath = path.relative(downloadDir, actualFilePath);
        const onedrivePath = relativePath.replace(/\\/g, '/');
        
        console.log('Uploading:', onedrivePath);
        await uploadToOneDrive(actualFilePath, onedrivePath, actualFileStats.size, accessToken, rootPath);
        uploadedFiles.push({ name: onedrivePath, size: actualFileStats.size });
        
        // Delete uploaded file
        downloadedItems.forEach(f => {
            const fp = path.join(downloadDir, f);
            fs.statSync(fp).isDirectory() ? fs.rmSync(fp, { recursive: true, force: true }) : fs.unlinkSync(fp);
        });
        console.log('[' + (i + 1) + '/' + fileCount + '] Done');
    }
    
    console.log('All', uploadedFiles.length, 'files uploaded!');
    return uploadedFiles;
}

async function uploadToOneDrive(filePath, fileName, fileSize, accessToken, basePath) {
    const safeName = fileName.replace(/\\/g, '/');
    if (fileSize <= 4 * 1024 * 1024) {
        await axios.put('https://graph.microsoft.com/v1.0/me/drive/root:/' + basePath + '/' + safeName + ':/content', fs.readFileSync(filePath), { headers: { 'Authorization': 'Bearer ' + accessToken } });
    } else {
        const session = await createUploadSession(accessToken, basePath, safeName);
        let uploaded = 0;
        while (uploaded < fileSize) {
            const end = Math.min(uploaded + CHUNK_SIZE, fileSize);
            await uploadChunk(session.uploadUrl, filePath, uploaded, end, fileSize);
            uploaded = end;
            if (uploaded % (100 * 1024 * 1024) < CHUNK_SIZE) console.log('Progress:', (uploaded / 1024 / 1024).toFixed(0), 'MB');
        }
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
    await axios.put(uploadUrl, buffer, { headers: { 'Content-Length': end - start, 'Content-Range': 'bytes ' + start + '-' + (end - 1) + '/' + totalSize }, maxBodyLength: Infinity, maxContentLength: Infinity });
}

async function refreshAccessToken(clientId, clientSecret, tenantId, refreshToken) {
    const response = await axios.post('https://login.microsoftonline.com/' + tenantId + '/oauth2/v2.0/token', new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
    return response.data.access_token;
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

// 创建进度报告器
function createProgressReporter(progressUrl, taskId) {
    let lastReport = 0;
    const minInterval = 10000; // 最少10秒报告一次
    
    return async (data) => {
        if (!progressUrl || !taskId) return;
        const now = Date.now();
        if (now - lastReport < minInterval) return;
        lastReport = now;
        
        try {
            await axios.post(progressUrl, { taskId, ...data }, { timeout: 5000 });
        } catch (e) {
            // 忽略报告错误
        }
    };
}

// 解析 aria2c 输出中的进度信息
function parseAria2Progress(str) {
    // [#ae93e1 468MiB/1.7GiB(26%) CN:37 SD:6 DL:19MiB ETA:1m6s]
    const match = str.match(/\[#\w+\s+([\d.]+\w+)\/([\d.]+\w+)\((\d+)%\).*?DL:([\d.]+\w+).*?ETA:([^\]]+)\]/);
    if (match) {
        return {
            downloaded: match[1],
            total: match[2],
            percent: parseInt(match[3]),
            speed: match[4],
            eta: match[5],
            progress: `${match[1]}/${match[2]} (${match[3]}%) DL:${match[4]} ETA:${match[5]}`
        };
    }
    return null;
}

main().catch(err => { console.error('Fatal error:', err); process.exit(1); });
