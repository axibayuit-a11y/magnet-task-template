// stream_upload.js - 边下边传脚本
const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const CHUNK_SIZE = 30 * 1024 * 1024; // 30MB = 320KB × 96
const POLL_INTERVAL = 3000;
const MIN_FREE_SPACE = 2 * 1024 * 1024 * 1024; // 2GB
const LARGE_FILE_THRESHOLD = 14 * 1024 * 1024 * 1024; // 14GB

class Aria2RPC {
    constructor(port = 6800) {
        this.url = `http://localhost:${port}/jsonrpc`;
    }

    async call(method, params = []) {
        const resp = await axios.post(this.url, {
            jsonrpc: '2.0',
            id: Date.now(),
            method: `aria2.${method}`,
            params: params
        });
        return resp.data.result;
    }
}

async function main() {
    const magnet = process.env.MAGNET;
    const refreshToken = process.env.OD_REFRESH_TOKEN;
    const clientId = process.env.OD_CLIENT_ID;
    const clientSecret = process.env.OD_CLIENT_SECRET;
    const tenantId = process.env.OD_TENANT_ID;
    const callbackUrl = process.env.CALLBACK_URL;
    const taskId = process.env.TASK_ID;
    const targetFolder = process.env.TARGET_FOLDER || 'downloads';

    if (!magnet) {
        console.error('❌ MAGNET environment variable is required');
        process.exit(1);
    }

    console.log('🚀 Starting magnet download...');
    console.log(`📥 Magnet: ${magnet.substring(0, 60)}...`);

    // 1. 刷新 OneDrive Access Token
    const accessToken = await refreshAccessToken(clientId, clientSecret, tenantId, refreshToken);
    console.log('✅ OneDrive token refreshed');

    // 2. 启动 aria2
    const downloadDir = './downloads';
    fs.mkdirSync(downloadDir, { recursive: true });

    const trackers = process.env.BT_TRACKERS || '';
    console.log(`📡 Using ${trackers.split(',').filter(t => t).length} trackers`);

    const aria2Args = [
        '--enable-rpc',
        '--rpc-listen-port=6800',
        '--file-allocation=none',
        '--seed-time=0',
        '--dir=' + downloadDir,
        '--max-connection-per-server=16',
        '--bt-max-peers=100',
        '--bt-request-peer-speed-limit=10M',
        '--summary-interval=10',
    ];

    if (trackers) {
        aria2Args.push(`--bt-tracker=${trackers}`);
    }

    aria2Args.push(magnet);

    console.log('🚀 Starting aria2...');
    const aria2 = spawn('aria2c', aria2Args);
    const aria2rpc = new Aria2RPC();

    aria2.stdout.on('data', (data) => console.log(data.toString()));
    aria2.stderr.on('data', (data) => console.error(data.toString()));

    await sleep(5000);

    // 3. 获取下载任务信息
    let gid = null;
    let filePath = null;
    let totalSize = 0;
    let useSequential = false;
    let retries = 0;

    while (!gid && retries < 60) {
        try {
            const active = await aria2rpc.call('tellActive');
            if (active.length > 0) {
                gid = active[0].gid;
                totalSize = parseInt(active[0].totalLength);
                if (active[0].files && active[0].files[0]) {
                    filePath = active[0].files[0].path;
                }

                if (totalSize > LARGE_FILE_THRESHOLD) {
                    console.log(`⚠️ Large file (${(totalSize/1024/1024/1024).toFixed(2)}GB), switching to sequential mode`);
                    await aria2rpc.call('changeOption', [gid, {
                        'stream-piece-selector': 'inorder',
                        'bt-prioritize-piece': 'head'
                    }]);
                    useSequential = true;
                }
            }
        } catch (e) {
            // RPC not ready yet
        }
        retries++;
        await sleep(2000);
    }

    if (!gid) {
        console.error('❌ Failed to start download');
        process.exit(1);
    }

    console.log(`📁 File: ${path.basename(filePath || 'unknown')}`);
    console.log(`📊 Size: ${(totalSize/1024/1024/1024).toFixed(2)} GB`);
    console.log(`📥 Mode: ${useSequential ? 'Sequential' : 'Random'}`);

    // 4. 创建 OneDrive 上传会话
    const fileName = path.basename(filePath || `download_${Date.now()}`);
    const uploadSession = await createUploadSession(accessToken, targetFolder, fileName, totalSize);
    const uploadUrl = uploadSession.uploadUrl;
    console.log('✅ Upload session created');

    // 5. 边下边传主循环
    let uploadedBytes = 0;
    let isPaused = false;
    let isCompleted = false;

    const uploadLoop = setInterval(async () => {
        try {
            const status = await aria2rpc.call('tellStatus', [gid]);
            
            if (status.status === 'complete') {
                isCompleted = true;
                return;
            }

            // 水位线检测
            const freeSpace = getFreeSpace();
            if (freeSpace < MIN_FREE_SPACE && !isPaused) {
                console.log(`⚠️ Low disk space (${(freeSpace/1024/1024/1024).toFixed(2)}GB), pausing download`);
                await aria2rpc.call('pause', [gid]);
                isPaused = true;
            }

            const bitfield = status.bitfield || '';
            const pieceLength = parseInt(status.pieceLength) || CHUNK_SIZE;
            const uploadableEnd = getUploadablePrefix(bitfield, pieceLength, totalSize);

            while (uploadedBytes + CHUNK_SIZE <= uploadableEnd) {
                const chunkEnd = uploadedBytes + CHUNK_SIZE;
                await uploadChunk(uploadUrl, filePath, uploadedBytes, chunkEnd, totalSize);
                
                if (useSequential) {
                    truncateFile(filePath, uploadedBytes, CHUNK_SIZE);
                }
                
                uploadedBytes = chunkEnd;
                console.log(`📤 Uploaded: ${(uploadedBytes/1024/1024).toFixed(0)} MB / ${(totalSize/1024/1024).toFixed(0)} MB`);

                if (isPaused) {
                    const newFreeSpace = getFreeSpace();
                    if (newFreeSpace > MIN_FREE_SPACE * 2) {
                        console.log('✅ Disk space recovered, resuming download');
                        await aria2rpc.call('unpause', [gid]);
                        isPaused = false;
                    }
                }
            }
        } catch (err) {
            console.error('Loop error:', err.message);
        }
    }, POLL_INTERVAL);

    // 6. 等待完成
    await new Promise((resolve) => {
        aria2.on('close', resolve);
        const checkComplete = setInterval(() => {
            if (isCompleted) {
                clearInterval(checkComplete);
                resolve();
            }
        }, 1000);
    });

    clearInterval(uploadLoop);
    console.log('✅ Download complete');

    // 7. 上传剩余部分
    while (uploadedBytes < totalSize) {
        const chunkEnd = Math.min(uploadedBytes + CHUNK_SIZE, totalSize);
        await uploadChunk(uploadUrl, filePath, uploadedBytes, chunkEnd, totalSize);
        uploadedBytes = chunkEnd;
        console.log(`📤 Final: ${(uploadedBytes/1024/1024).toFixed(0)} MB / ${(totalSize/1024/1024).toFixed(0)} MB`);
    }

    console.log('✅ Upload complete!');

    // 8. 回调
    if (callbackUrl) {
        await axios.post(callbackUrl, {
            taskId: taskId,
            status: 'completed',
            fileName: fileName,
            fileSize: totalSize
        });
    }
}

function getUploadablePrefix(hexBitfield, pieceLength, totalSize) {
    if (!hexBitfield) return 0;
    
    let binary = '';
    for (const char of hexBitfield) {
        binary += parseInt(char, 16).toString(2).padStart(4, '0');
    }
    
    let continuousPieces = 0;
    for (const bit of binary) {
        if (bit === '1') {
            continuousPieces++;
        } else {
            break;
        }
    }
    
    return Math.min(continuousPieces * pieceLength, totalSize);
}

function getFreeSpace() {
    try {
        const output = execSync("df -B1 . | tail -1 | awk '{print $4}'").toString().trim();
        return parseInt(output);
    } catch {
        return Infinity;
    }
}

function truncateFile(filePath, offset, length) {
    try {
        execSync(`fallocate -p -o ${offset} -l ${length} "${filePath}"`, { stdio: 'ignore' });
    } catch {
        // Ignore
    }
}

async function refreshAccessToken(clientId, clientSecret, tenantId, refreshToken) {
    const resp = await axios.post(
        `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
        new URLSearchParams({
            client_id: clientId,
            client_secret: clientSecret,
            refresh_token: refreshToken,
            grant_type: 'refresh_token'
        }),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );
    return resp.data.access_token;
}

async function createUploadSession(accessToken, folder, fileName, fileSize) {
    const filePath = `${folder}/${fileName}`;
    const resp = await axios.post(
        `https://graph.microsoft.com/v1.0/me/drive/root:/${filePath}:/createUploadSession`,
        { 
            item: { 
                '@microsoft.graph.conflictBehavior': 'rename',
                name: fileName
            }
        },
        { headers: { 'Authorization': `Bearer ${accessToken}` } }
    );
    return resp.data;
}

async function uploadChunk(uploadUrl, filePath, start, end, totalSize) {
    const fd = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(end - start);
    fs.readSync(fd, buffer, 0, end - start, start);
    fs.closeSync(fd);

    await axios.put(uploadUrl, buffer, {
        headers: {
            'Content-Length': end - start,
            'Content-Range': `bytes ${start}-${end - 1}/${totalSize}`
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

main().catch(err => {
    console.error('❌ Fatal error:', err);
    process.exit(1);
});
