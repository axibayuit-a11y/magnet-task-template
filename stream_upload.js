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

    // 进度报告函数
    const reportProgress = createProgressReporter(progressUrl, taskId);

    let uploadedFiles = [];
    const maxTime = maxTimeHours * 3600000;
    const stallTimeout = stallTimeoutMinutes * 60000;
    
    if (mode === 'normal') {
        uploadedFiles = await normalDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress);
    } else if (mode === 'streaming') {
        uploadedFiles = await streamingDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress);
    } else {
        uploadedFiles = await sequentialDownloadAndUpload(magnet, trackers, downloadDir, torrentName, fileList, fileCount, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress);
    }

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

async function normalDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress) {
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
    // 报告下载完成，开始上传
    if (reportProgress) {
        reportProgress({ phase: 'uploading', progress: '下载完成，开始上传...', percent: 0 });
    }
    
    const items = fs.readdirSync(downloadDir).filter(f => !f.endsWith('.torrent') && !f.endsWith('.aria2'));
    if (items.length === 0) throw new Error('No files found');
    
    const firstItem = path.join(downloadDir, items[0]);
    const stats = fs.statSync(firstItem);
    const uploadedFiles = [];
    
    if (stats.isDirectory()) {
        // 多文件：磁力原有结构是 torrentName/file.mkv
        const torrentName = items[0];
        const allFiles = getAllFiles(firstItem);
        console.log('Multi-file:', allFiles.length, 'files');
        for (let i = 0; i < allFiles.length; i++) {
            const file = allFiles[i];
            const fileStats = fs.statSync(file);
            const relativePath = path.relative(firstItem, file).replace(/\\/g, '/');
            // OneDrive: onedrivePath/torrentName/relativePath
            const fileOnedrivePath = torrentName + '/' + relativePath;
            // KV: kvBasePath/torrentName/relativePath (不含日期)
            const fileKvPath = kvBasePath ? kvBasePath + '/' + torrentName + '/' + relativePath : torrentName + '/' + relativePath;
            
            console.log('[' + (i + 1) + '/' + allFiles.length + '] Uploading:', relativePath);
            
            // 报告上传进度
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
            
            const uploadResult = await uploadToOneDrive(file, fileOnedrivePath, fileStats.size, accessToken, onedrivePath);
            uploadedFiles.push({ 
                name: relativePath, 
                size: fileStats.size,
                itemId: uploadResult.itemId,
                onedrivePath: uploadResult.path,
                kvPath: fileKvPath
            });
        }
        // 上传完成
        if (reportProgress) {
            reportProgress({ phase: 'completed', progress: `上传完成 ${allFiles.length} 个文件`, percent: 100 });
        }
    } else {
        // 单文件：直接放在 kvBasePath 下
        const fileName = items[0];
        // KV: kvBasePath/fileName (不含日期)
        const fileKvPath = kvBasePath ? kvBasePath + '/' + fileName : fileName;
        
        if (reportProgress) {
            reportProgress({ phase: 'uploading', progress: `上传中: ${fileName}`, percent: 50 });
        }
        const uploadResult = await uploadToOneDrive(firstItem, fileName, stats.size, accessToken, onedrivePath);
        uploadedFiles.push({ 
            name: fileName, 
            size: stats.size,
            itemId: uploadResult.itemId,
            onedrivePath: uploadResult.path,
            kvPath: fileKvPath
        });
        if (reportProgress) {
            reportProgress({ phase: 'completed', progress: '上传完成', percent: 100 });
        }
    }
    return uploadedFiles;
}

async function streamingDownloadAndUpload(magnet, trackers, downloadDir, totalSize, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress) {
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

    const uploadSession = await createUploadSession(accessToken, onedrivePath, actualFileName);
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
    let lastChunkResponse = null;
    while (uploadedBytes < finalSize) {
        const chunkEnd = Math.min(uploadedBytes + CHUNK_SIZE, finalSize);
        lastChunkResponse = await uploadChunk(uploadUrl, actualFile, uploadedBytes, chunkEnd, finalSize);
        uploadedBytes = chunkEnd;
        // 报告上传进度
        if (reportProgress) {
            const percent = Math.round((uploadedBytes / finalSize) * 100);
            reportProgress({ 
                phase: 'streaming-upload', 
                progress: `流式上传 ${(uploadedBytes / 1024 / 1024).toFixed(0)}MB / ${(finalSize / 1024 / 1024).toFixed(0)}MB`,
                percent,
                uploaded: uploadedBytes,
                total: finalSize
            });
        }
    }
    console.log('Upload complete!');
    if (reportProgress) {
        reportProgress({ phase: 'completed', progress: '上传完成', percent: 100 });
    }
    
    // 单文件流式上传
    const fileKvPath = kvBasePath ? kvBasePath + '/' + actualFileName : actualFileName;
    return [{ 
        name: actualFileName, 
        size: finalSize,
        itemId: lastChunkResponse?.id || '',
        onedrivePath: onedrivePath + '/' + actualFileName,
        kvPath: fileKvPath
    }];
}

async function sequentialDownloadAndUpload(magnet, trackers, downloadDir, torrentName, fileList, fileCount, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress) {
    console.log('[Sequential] File-by-file download...');
    const uploadedFiles = [];
    const startTime = Date.now();
    
    if (fileList.length === 0) {
        console.log('Getting file list...');
        if (reportProgress) {
            reportProgress({ phase: 'metadata', progress: '获取文件列表...', percent: 0 });
        }
        
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
            return normalDownloadAndUpload(magnet, trackers, downloadDir, 0, accessToken, onedrivePath, kvBasePath, maxTime, stallTimeout, reportProgress);
        }
    }
    
    for (let i = 0; i < fileList.length; i++) {
        if (Date.now() - startTime > maxTime) {
            throw new Error('Max time exceeded');
        }
        
        const fileInfo = fileList[i];
        const fileName = path.basename(fileInfo.path);
        console.log('[' + (i + 1) + '/' + fileCount + '] Downloading file', fileInfo.index);
        
        // 报告开始下载此文件
        if (reportProgress) {
            const overallPercent = Math.round((i / fileCount) * 100);
            reportProgress({ 
                phase: 'sequential-download', 
                progress: `[${i + 1}/${fileCount}] 下载: ${fileName}`,
                percent: overallPercent,
                fileIndex: i + 1,
                fileCount,
                currentFile: fileName
            });
        }
        
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
                    reportProgress({ 
                        ...progressInfo, 
                        phase: 'sequential-download', 
                        fileIndex: i + 1, 
                        fileCount,
                        currentFile: fileName,
                        progress: `[${i + 1}/${fileCount}] ${progressInfo.progress}`
                    });
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
        // relativePath 是相对于 downloadDir 的路径，保持磁力原有结构
        const relativePath = path.relative(downloadDir, actualFilePath).replace(/\\/g, '/');
        
        console.log('Uploading:', relativePath);
        
        // 报告开始上传此文件
        if (reportProgress) {
            const overallPercent = Math.round(((i + 0.5) / fileCount) * 100);
            reportProgress({ 
                phase: 'sequential-upload', 
                progress: `[${i + 1}/${fileCount}] 上传: ${fileName}`,
                percent: overallPercent,
                fileIndex: i + 1,
                fileCount,
                currentFile: fileName
            });
        }
        
        // OneDrive: onedrivePath/relativePath
        // KV: kvBasePath/relativePath (不含日期)
        const fileKvPath = kvBasePath ? kvBasePath + '/' + relativePath : relativePath;
        
        const uploadResult = await uploadToOneDrive(actualFilePath, relativePath, actualFileStats.size, accessToken, onedrivePath);
        uploadedFiles.push({ 
            name: relativePath, 
            size: actualFileStats.size,
            itemId: uploadResult.itemId,
            onedrivePath: uploadResult.path,
            kvPath: fileKvPath
        });
        
        // Delete uploaded file
        downloadedItems.forEach(f => {
            const fp = path.join(downloadDir, f);
            fs.statSync(fp).isDirectory() ? fs.rmSync(fp, { recursive: true, force: true }) : fs.unlinkSync(fp);
        });
        console.log('[' + (i + 1) + '/' + fileCount + '] Done');
    }
    
    // 报告全部完成
    if (reportProgress) {
        reportProgress({ phase: 'completed', progress: `全部完成 ${uploadedFiles.length} 个文件`, percent: 100 });
    }
    
    console.log('All', uploadedFiles.length, 'files uploaded!');
    return uploadedFiles;
}

async function uploadToOneDrive(filePath, fileName, fileSize, accessToken, basePath) {
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
        while (uploaded < fileSize) {
            const end = Math.min(uploaded + CHUNK_SIZE, fileSize);
            lastResponse = await uploadChunk(session.uploadUrl, filePath, uploaded, end, fileSize);
            uploaded = end;
            if (uploaded % (100 * 1024 * 1024) < CHUNK_SIZE) console.log('Progress:', (uploaded / 1024 / 1024).toFixed(0), 'MB');
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

// 清理文件夹名（移除特殊字符）
function sanitizeFolderName(name) {
    if (!name) return '';
    
    let cleaned = name
        .replace(/[<>:"/\\|?*\x00-\x1f]/g, '')  // 移除 Windows/OneDrive 不允许的字符
        .replace(/\s+/g, ' ')                    // 多个空格合并
        .replace(/^\.+/, '')                     // 移除开头的点
        .trim()
        .substring(0, 200);                      // 限制长度
    
    return cleaned || '';
}

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
