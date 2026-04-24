/* DocFusion Frontend Application v2.0 */
const API_BASE = '';
const STEP_ORDER = ['cleanup', 'parse', 'requirement', 'template', 'retrieve', 'extract', 'fill', 'validate', 'output'];

// ── State ─────────────────────────────────────────────────────────────────────
let sourceFiles = [];
let templateFiles = [];
let currentTaskId = '';
let displayedLogCount = 0;
let seenWarnings = new Set();
let requestStartedWithEmptyRequirement = false;
let latestTaskSnapshot = null;
let completedResults = [];

// New state for enhanced features
let externalSources = [];   // [{type, name, url, db_path, query}]
let operationFilePath = '';  // Current file for doc operations
let storeFilePaths = [];     // Files staged for store import
let dashboardSnapshot = null;
let evaluationResults = [];

// ── Initialization ─────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
    setupUploadZones();
    setupOperationFileZone();
    setupStoreFileZone();
    renderExternalSources();
    loadAnalyticsDashboard();
});

// ── Upload zones ──────────────────────────────────────────────────────────────
function setupUploadZones() {
    setupDropZone('source-drop', 'source-input', 'source-list', 'source');
    setupDropZone('template-drop', 'template-input', 'template-list', 'template');
}

function setupDropZone(dropId, inputId, listId, type) {
    const drop = document.getElementById(dropId);
    const input = document.getElementById(inputId);
    if (!drop || !input) return;

    drop.addEventListener('click', () => input.click());
    drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag-over'); });
    drop.addEventListener('dragleave', () => drop.classList.remove('drag-over'));
    drop.addEventListener('drop', (e) => {
        e.preventDefault();
        drop.classList.remove('drag-over');
        addFiles(e.dataTransfer.files, type);
    });
    input.addEventListener('change', (e) => { addFiles(e.target.files, type); input.value = ''; });
}

function setupOperationFileZone() {
    const drop = document.getElementById('op-file-drop');
    const input = document.getElementById('op-file-input');
    if (!drop || !input) return;

    drop.addEventListener('click', () => input.click());
    drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag-over'); });
    drop.addEventListener('dragleave', () => drop.classList.remove('drag-over'));
    drop.addEventListener('drop', (e) => {
        e.preventDefault();
        drop.classList.remove('drag-over');
        handleOperationFileSelect(e.dataTransfer.files[0]);
    });
    input.addEventListener('change', (e) => {
        if (e.target.files[0]) handleOperationFileSelect(e.target.files[0]);
        input.value = '';
    });
}

function setupStoreFileZone() {
    const drop = document.getElementById('store-file-drop');
    const input = document.getElementById('store-file-input');
    if (!drop || !input) return;

    drop.addEventListener('click', () => input.click());
    drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag-over'); });
    drop.addEventListener('dragleave', () => drop.classList.remove('drag-over'));
    drop.addEventListener('drop', (e) => {
        e.preventDefault();
        drop.classList.remove('drag-over');
        storeFilePaths = [];
        Array.from(e.dataTransfer.files).forEach(f => { if (!storeFilePaths.find(x => x.name === f.name)) storeFilePaths.push(f); });
        updateStoreFileInfo();
    });
    input.addEventListener('change', (e) => {
        storeFilePaths = [];
        Array.from(e.target.files).forEach(f => { if (!storeFilePaths.find(x => x.name === f.name)) storeFilePaths.push(f); });
        updateStoreFileInfo();
        input.value = '';
    });
}

function updateStoreFileInfo() {
    const drop = document.getElementById('store-file-drop');
    if (storeFilePaths.length > 0) {
        const names = storeFilePaths.map(f => f.name).join(', ');
        drop.querySelector('span:last-of-type').textContent = `已选 ${storeFilePaths.length} 个文件: ${truncateText(names, 40)}`;
    }
}

function addFiles(fileList, type) {
    const arr = type === 'source' ? sourceFiles : templateFiles;
    const listId = type === 'source' ? 'source-list' : 'template-list';
    for (const f of fileList) {
        if (!arr.find(x => x.name === f.name)) arr.push(f);
    }
    renderFileList(arr, listId, type);
}

function renderFileList(files, listId, type) {
    const list = document.getElementById(listId);
    if (!list) return;
    list.innerHTML = '';
    files.forEach((f, i) => {
        const div = document.createElement('div');
        div.className = 'file-item';
        div.innerHTML = `<span class="file-name">${escHtml(f.name)}</span><span class="file-size">${formatSize(f.size)}</span><span class="file-remove" onclick="removeFile('${type}', ${i})">✕</span>`;
        list.appendChild(div);
    });
}

function removeFile(type, index) {
    if (type === 'source') { sourceFiles.splice(index, 1); renderFileList(sourceFiles, 'source-list', 'source'); }
    else { templateFiles.splice(index, 1); renderFileList(templateFiles, 'template-list', 'template'); }
}

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(1) + ' MB';
}

// ── Health Check ───────────────────────────────────────────────────────────────
async function checkHealth() {
    const dot = document.querySelector('.status-dot');
    const info = document.getElementById('model-info');
    if (!dot || !info) return;
    dot.className = 'status-dot checking';
    info.textContent = '检查模型状态...';
    try {
        const res = await fetch(API_BASE + '/api/health');
        const data = await res.json();
        if (data.ollama_status === 'ok') {
            dot.className = 'status-dot online';
            info.textContent = `后端可用: ${data.provider || 'ollama'} / 模型 ${data.model}`;
        } else {
            dot.className = 'status-dot offline';
            info.textContent = `模型异常: ${data.ollama_status}`;
        }
    } catch (e) {
        dot.className = 'status-dot offline';
        info.textContent = '后端连接失败';
    }
}

// ── Template Inspection ───────────────────────────────────────────────────────
async function inspectUploadedTemplate() {
    if (templateFiles.length === 0) { alert('请先上传模板文件'); return; }
    const hint = document.getElementById('template-schema-hint');
    const panel = document.getElementById('template-schema-panel');
    hint.textContent = '解析中...';
    try {
        const uploaded = await uploadFiles('/api/templates/upload', [templateFiles[0]]);
        const filePath = uploaded[0].path;
        const res = await fetch(API_BASE + '/api/templates/inspect/local', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_path: filePath }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            hint.textContent = `解析成功: ${data.filename}`;
            renderTemplateSchema(data.schema, panel);
        } else {
            hint.textContent = `解析失败: ${data.detail || '未知错误'}`;
        }
    } catch (e) {
        hint.textContent = `错误: ${e.message}`;
    }
}

function renderTemplateSchema(schema, container) {
    if (!schema || !container) return;
    container.style.display = '';
    const tables = schema.tables || [];
    const fields = schema.fields || [];
    const placeholders = schema.placeholders || [];
    container.innerHTML = `
        <div style="font-size:0.85rem;border:1px solid var(--border);border-radius:8px;padding:0.75rem;background:#f0f9ff">
            <div style="font-weight:600;margin-bottom:0.5rem">模板结构预览</div>
            <div>类型：${escHtml(schema.file_type || '')} | 结构：${escHtml(schema.structure_type || '')} | 表格数：${tables.length} | 字段数：${fields.length}</div>
            ${tables.length ? `<div style="margin-top:0.5rem"><strong>表格表头：</strong>${tables.map(t => `<span style="background:#dbeafe;padding:0.1rem 0.4rem;border-radius:3px;margin:0 0.2rem;font-size:0.78rem">${escHtml((t.headers || []).join(' | '))}</span>`).join('')}</div>` : ''}
            ${placeholders.length ? `<div style="margin-top:0.35rem"><strong>占位符：</strong>${placeholders.map(p => `<span style="background:#fef9c3;padding:0.1rem 0.4rem;border-radius:3px;margin:0 0.2rem;font-size:0.78rem">{{${escHtml(p)}}}</span>`).join('')}</div>` : ''}
        </div>
    `;
}

// ── Multi-source Management ────────────────────────────────────────────────────
function onSourceTypeChange() {
    const type = document.getElementById('new-source-type').value;
    const urlField = document.getElementById('source-url-field');
    const dbField = document.getElementById('source-db-field');
    const queryField = document.getElementById('source-query-field');
    if (type === 'database') {
        urlField.style.display = 'none';
        dbField.style.display = '';
        queryField.style.display = '';
    } else {
        urlField.style.display = '';
        dbField.style.display = 'none';
        queryField.style.display = 'none';
    }
}

async function previewExternalSource() {
    const type = document.getElementById('new-source-type').value;
    const name = document.getElementById('new-source-name').value.trim();
    const url = document.getElementById('new-source-url').value.trim();
    const dbPath = document.getElementById('new-source-db-path').value.trim();
    const query = document.getElementById('new-source-query').value.trim();

    const spec = buildSourceSpec(type, name, url, dbPath, query);
    if (!spec) return;

    const resultDiv = document.getElementById('source-preview-result');
    resultDiv.style.display = '';
    resultDiv.innerHTML = '<span style="color:var(--text-secondary)">预览中...</span>';

    try {
        const res = await fetch(API_BASE + '/api/sources/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source: spec, max_rows: 10 }),
        });
        const data = await res.json();
        const preview = data.previews && data.previews[0];
        if (!preview) { resultDiv.innerHTML = '<span style="color:var(--danger)">无预览数据</span>'; return; }
        if (preview.status === 'error') {
            resultDiv.innerHTML = `<span style="color:var(--danger)">预览失败: ${escHtml(preview.error || '')}</span>`;
            return;
        }
        let html = `<div style="margin-bottom:0.4rem"><strong>${escHtml(preview.source_name || spec.name || type)}</strong> · 类型 ${escHtml(preview.file_type || '')} · 文本块 ${preview.text_blocks} · 表格 ${preview.tables}</div>`;
        if (preview.table_previews && preview.table_previews.length > 0) {
            const tp = preview.table_previews[0];
            html += `<div style="overflow-x:auto"><table class="fields-table"><tr>${(tp.headers || []).map(h => `<th>${escHtml(h)}</th>`).join('')}</tr>`;
            (tp.rows || []).slice(0, 5).forEach(row => {
                html += `<tr>${row.map(c => `<td>${escHtml(String(c ?? ''))}</td>`).join('')}</tr>`;
            });
            html += '</table></div>';
        } else if (preview.raw_text_preview) {
            html += `<div style="color:var(--text-secondary)">${escHtml(preview.raw_text_preview.slice(0, 300))}...</div>`;
        }
        resultDiv.innerHTML = html;
    } catch (e) {
        resultDiv.innerHTML = `<span style="color:var(--danger)">请求失败: ${escHtml(e.message)}</span>`;
    }
}

function buildSourceSpec(type, name, url, dbPath, query) {
    const spec = { source_type: type, name: name || type };
    if (type === 'database') {
        if (!dbPath) { alert('请填写数据库路径'); return null; }
        spec.database_path = dbPath;
        spec.query = query || 'SELECT * FROM sqlite_master LIMIT 10';
    } else {
        if (!url) { alert('请填写 URL 地址'); return null; }
        spec.url = url;
        spec.method = 'GET';
    }
    return spec;
}

function addExternalSource() {
    const type = document.getElementById('new-source-type').value;
    const name = document.getElementById('new-source-name').value.trim();
    const url = document.getElementById('new-source-url').value.trim();
    const dbPath = document.getElementById('new-source-db-path').value.trim();
    const query = document.getElementById('new-source-query').value.trim();
    const spec = buildSourceSpec(type, name, url, dbPath, query);
    if (!spec) return;
    externalSources.push(spec);
    renderExternalSources();
    // Clear form
    document.getElementById('new-source-name').value = '';
    document.getElementById('new-source-url').value = '';
    document.getElementById('new-source-db-path').value = '';
    document.getElementById('new-source-query').value = '';
    document.getElementById('source-preview-result').style.display = 'none';
}

function removeExternalSource(index) {
    externalSources.splice(index, 1);
    renderExternalSources();
}

function renderExternalSources() {
    const list = document.getElementById('external-sources-list');
    if (!list) return;
    if (externalSources.length === 0) {
        list.innerHTML = '<div style="color:var(--text-secondary);font-size:0.82rem;margin-bottom:0.5rem">暂无外部数据源。可在下方添加 HTTP API / 网页 / SQLite 来源。</div>';
        return;
    }
    list.innerHTML = externalSources.map((src, i) => {
        const typeClass = src.source_type === 'web_page' ? 'web' : src.source_type === 'database' ? 'db' : '';
        const addr = src.url || src.database_path || '';
        return `<div class="ext-source-item">
            <span class="ext-source-type ${typeClass}">${escHtml(src.source_type)}</span>
            <span style="flex:1">${escHtml(src.name || src.source_type)} — <span style="color:var(--text-secondary)">${escHtml(truncateText(addr, 50))}</span></span>
            <span class="file-remove" onclick="removeExternalSource(${i})">✕</span>
        </div>`;
    }).join('');
}

// ── Document Operation ─────────────────────────────────────────────────────────
function handleOperationFileSelect(file) {
    if (!file) return;
    operationFilePath = null; // will be set after upload
    const info = document.getElementById('op-file-info');
    info.textContent = `已选: ${file.name} (${formatSize(file.size)}) — 执行操作时将自动上传`;
    // Store the file object for later
    window._opFileObject = file;
    // Show extra fields based on current op type
    onOpTypeChange();
}

function onOpTypeChange() {
    const type = document.getElementById('op-type').value;
    const extraDiv = document.getElementById('op-extra-fields');
    const fieldsRow = document.getElementById('op-fields-row');
    const replacementsRow = document.getElementById('op-replacements-row');
    if (type === 'extract') {
        extraDiv.style.display = '';
        fieldsRow.style.display = '';
        replacementsRow.style.display = 'none';
    } else if (type === 'replace') {
        extraDiv.style.display = '';
        fieldsRow.style.display = 'none';
        replacementsRow.style.display = '';
    } else {
        extraDiv.style.display = 'none';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const opTypeSelect = document.getElementById('op-type');
    if (opTypeSelect) opTypeSelect.addEventListener('change', onOpTypeChange);
});

function parseFieldsInput(value) {
    if (!value || !value.trim()) return [];
    return value.split(/[,，、;；]/).map(s => s.trim()).filter(Boolean);
}

function parseReplacementsInput(value) {
    if (!value || !value.trim()) return {};
    try { return JSON.parse(value); } catch (e) {
        // Parse key=value or key:value pairs
        const result = {};
        value.split(/[,，;；\n]/).forEach(pair => {
            const m = pair.match(/^([^:：=]+)[：:=]\s*(.+)$/);
            if (m) result[m[1].trim()] = m[2].trim();
        });
        return result;
    }
}

async function startDocumentOperation() {
    const instruction = document.getElementById('op-instruction').value.trim();
    const opType = document.getElementById('op-type').value;
    const useLLM = document.getElementById('op-use-llm').checked;
    const statusText = document.getElementById('op-status-text');
    const resultPanel = document.getElementById('op-result-panel');
    const btn = document.getElementById('op-btn');

    if (!window._opFileObject && !operationFilePath) {
        alert('请先上传待操作文档');
        return;
    }
    if (!instruction && !opType) {
        alert('请输入操作指令或选择操作类型');
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 处理中...';
    statusText.textContent = '';
    resultPanel.style.display = 'none';

    try {
        // Upload file if not yet uploaded
        if (!operationFilePath && window._opFileObject) {
            statusText.textContent = '上传文件中...';
            const uploaded = await uploadFiles('/api/files/upload', [window._opFileObject]);
            operationFilePath = uploaded[0].path;
        }

        statusText.textContent = '执行操作中...';
        const fields = parseFieldsInput(document.getElementById('op-fields-input').value);
        const replacements = parseReplacementsInput(document.getElementById('op-replacements-input').value);

        const body = {
            file_path: operationFilePath,
            instruction,
            operation: opType || null,
            use_llm: useLLM,
            fields: fields.length ? fields : null,
            replacements: Object.keys(replacements).length ? replacements : null,
        };

        const res = await fetch(API_BASE + '/api/document/operate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        renderDocumentOperationResult(data);
        statusText.textContent = '操作完成';
    } catch (e) {
        statusText.textContent = `操作失败: ${e.message}`;
        resultPanel.style.display = '';
        resultPanel.innerHTML = `<div class="alert alert-error">操作失败: ${escHtml(e.message)}</div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = '▶ 执行操作';
    }
}

function renderDocumentOperationResult(data) {
    const panel = document.getElementById('op-result-panel');
    panel.style.display = '';
    const result = data.result || {};
    const warnings = data.warnings || [];
    const outputFile = data.output_file || '';

    let html = `<div class="op-result-block">`;
    html += `<div style="font-weight:600;margin-bottom:0.5rem">操作类型: ${escHtml(data.operation || data.intent || '未知')} | 状态: ${escHtml(data.status || '')}</div>`;

    // Output file download
    if (outputFile) {
        const fname = outputFile.split('/').pop();
        html += `<div style="margin-bottom:0.5rem"><a class="download-btn" href="${API_BASE}/api/download/${encodeURIComponent(fname)}" target="_blank">⬇️ 下载结果文件: ${escHtml(fname)}</a></div>`;
    }

    // Render based on operation type
    const intent = data.intent || data.operation;
    if (intent === 'summarize' && result.key_points) {
        html += `<div style="margin-bottom:0.5rem"><strong>摘要：</strong>${escHtml(result.summary || '')}</div>`;
        html += `<div><strong>要点：</strong></div>`;
        (result.key_points || []).slice(0, 8).forEach(pt => {
            html += `<div class="op-key-point">▸ ${escHtml(pt)}</div>`;
        });
    } else if (intent === 'extract' && result.fields) {
        html += `<div><strong>提取结果：</strong></div>`;
        Object.entries(result.fields).forEach(([field, values]) => {
            html += `<div style="margin-top:0.4rem;font-weight:600">${escHtml(field)}（${values.length} 条）：</div>`;
            (values || []).slice(0, 10).forEach(v => {
                html += `<div style="padding:0.2rem 0.5rem;font-size:0.8rem;color:var(--text-secondary)">
                    ${escHtml(String(v.value ?? ''))}
                    <span style="font-size:0.72rem">[${escHtml(v.location || '')} · ${v.confidence != null ? Math.round(v.confidence*100)+'%' : ''}]</span>
                </div>`;
            });
        });
    } else if (intent === 'find' && result.matches) {
        html += `<div><strong>查找结果（${result.match_count || 0} 处）：</strong></div>`;
        (result.matches || []).slice(0, 20).forEach(m => {
            html += `<div class="op-key-point">[${escHtml(m.location || '')}] ${escHtml(m.snippet || '')}</div>`;
        });
    } else if (intent === 'replace') {
        const repl = result.replacements || {};
        html += `<div><strong>替换内容：</strong></div>`;
        Object.entries(repl).forEach(([k, v]) => {
            html += `<div style="font-size:0.82rem;padding:0.2rem 0">${escHtml(k)} → ${escHtml(v)}</div>`;
        });
    } else {
        // Generic result display
        const preview = JSON.stringify(result, null, 2);
        html += `<pre style="font-size:0.78rem;overflow:auto;max-height:300px;background:#0f172a;color:#e2e8f0;border-radius:6px;padding:0.5rem">${escHtml(preview.slice(0, 3000))}</pre>`;
    }

    // Warnings
    if (warnings.length > 0) {
        html += `<div style="margin-top:0.5rem;color:var(--warning);font-size:0.8rem">${warnings.map(w => `⚠️ ${escHtml(w)}`).join('<br>')}</div>`;
    }

    html += '</div>';
    panel.innerHTML = html;
}

// ── Store Import ──────────────────────────────────────────────────────────────
async function startStoreImport() {
    if (storeFilePaths.length === 0) { alert('请先选择要入库的文件'); return; }
    const extractEntities = document.getElementById('store-extract-entities').checked;
    const overwrite = document.getElementById('store-overwrite').checked;
    const statusDiv = document.getElementById('store-task-status');

    statusDiv.style.display = '';
    statusDiv.textContent = '上传并入库中...';

    try {
        const formData = new FormData();
        storeFilePaths.forEach(f => formData.append('files', f));
        formData.append('extract_entities', String(extractEntities));
        formData.append('overwrite', String(overwrite));

        const res = await fetch(API_BASE + '/api/store/import/upload', {
            method: 'POST',
            body: formData,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);

        statusDiv.textContent = `入库任务已创建: ${data.task_id} — 轮询进度中...`;
        await pollStoreTask(data.task_id, statusDiv);
    } catch (e) {
        statusDiv.textContent = `入库失败: ${e.message}`;
        statusDiv.style.background = '#fee2e2';
    }
}

async function pollStoreTask(taskId, statusDiv) {
    for (let attempt = 0; attempt < 300; attempt++) {
        await wait(1000);
        try {
            const res = await fetch(`${API_BASE}/api/store/status/${encodeURIComponent(taskId)}`);
            const data = await res.json();
            statusDiv.textContent = `[${data.task_id}] ${data.message || data.current_stage} (${Math.round((data.progress || 0) * 100)}%) 成功=${data.imported_count} 跳过=${data.skipped_count} 失败=${data.error_count}`;
            if (data.status === 'completed' || data.status === 'error') {
                statusDiv.style.background = data.status === 'completed' ? '#f0fdf4' : '#fee2e2';
                if (data.errors && data.errors.length) {
                    statusDiv.innerHTML += `<br><span style="color:var(--danger)">${data.errors.slice(0, 3).map(escHtml).join('<br>')}</span>`;
                }
                loadStoreDocs();
                return;
            }
        } catch (e) {
            // continue polling
        }
    }
}

async function loadStoreDocs() {
    const panel = document.getElementById('store-content-panel');
    panel.style.display = '';
    panel.innerHTML = '<div style="color:var(--text-secondary);font-size:0.85rem">加载中...</div>';

    try {
        const res = await fetch(API_BASE + '/api/store/documents?limit=50');
        const data = await res.json();
        if (!data.documents || data.documents.length === 0) {
            panel.innerHTML = '<div style="color:var(--text-secondary);font-size:0.85rem">暂无入库文档</div>';
            return;
        }
        let html = `<div style="font-weight:600;font-size:0.88rem;margin-bottom:0.5rem">已入库文档（共 ${data.count} 个）</div>`;
        html += `<div style="overflow-x:auto"><table class="fields-table">
            <tr><th>文档</th><th>类型</th><th>文本块</th><th>表格</th><th>实体</th><th>字段</th><th>质量问题</th><th>入库时间</th><th>操作</th></tr>`;
        data.documents.forEach(doc => {
            const qCount = doc.quality_issue_count;
            const qDisplay = qCount >= 1000 ? '1000+' : qCount;
            const docIdJson = JSON.stringify(doc.document_id);
            html += `<tr>
                <td title="${escHtml(doc.source_file)}">${escHtml(truncateText(doc.title || doc.source_name || '', 30))}</td>
                <td>${escHtml(doc.file_type || '')}</td>
                <td>${doc.text_block_count}</td>
                <td>${doc.table_count}</td>
                <td>${doc.entity_count}</td>
                <td>${doc.field_count}</td>
                <td title="${qCount >= 1000 ? '仅展示前 1000 条质量问题' : ''}">${qDisplay}</td>
                <td style="font-size:0.75rem">${escHtml((doc.created_at || '').slice(0, 16))}</td>
                <td style="white-space:nowrap">
                    <button onclick="viewStoredDocument(${docIdJson})" style="font-size:0.75rem;padding:0.15rem 0.4rem;margin:0.1rem;border:1px solid var(--border);border-radius:4px;cursor:pointer;background:#fff">详情</button>
                    <button onclick="exportStoredDocument(${docIdJson})" style="font-size:0.75rem;padding:0.15rem 0.4rem;margin:0.1rem;border:1px solid var(--border);border-radius:4px;cursor:pointer;background:#fff">导出</button>
                    <button onclick="checkoutStoredDocument(${docIdJson})" style="font-size:0.75rem;padding:0.15rem 0.4rem;margin:0.1rem;border:1px solid #93c5fd;border-radius:4px;cursor:pointer;background:#eff6ff;color:#1d4ed8">出库</button>
                    <button onclick="deleteStoredDocument(${docIdJson})" style="font-size:0.75rem;padding:0.15rem 0.4rem;margin:0.1rem;border:1px solid #fca5a5;border-radius:4px;cursor:pointer;background:#fef2f2;color:#dc2626">删除</button>
                </td>
            </tr>`;
        });
        html += '</table></div>';
        panel.innerHTML = html;
    } catch (e) {
        panel.innerHTML = `<div class="alert alert-error">加载失败: ${escHtml(e.message)}</div>`;
    }
}

async function searchStore() {
    const q = document.getElementById('store-search-input').value.trim();
    if (!q) { loadStoreDocs(); return; }

    const panel = document.getElementById('store-content-panel');
    panel.style.display = '';
    panel.innerHTML = '<div style="color:var(--text-secondary);font-size:0.85rem">搜索中...</div>';

    try {
        const res = await fetch(`${API_BASE}/api/store/search?q=${encodeURIComponent(q)}&limit=30`);
        const data = await res.json();
        if (!data.results || data.results.length === 0) {
            panel.innerHTML = `<div style="color:var(--text-secondary);font-size:0.85rem">未找到匹配结果</div>`;
            return;
        }
        let html = `<div style="font-weight:600;font-size:0.88rem;margin-bottom:0.5rem">搜索「${escHtml(q)}」结果（${data.count} 条）</div>`;
        html += '<div style="border:1px solid var(--border);border-radius:8px;overflow:hidden">';
        data.results.forEach(r => {
            html += `<div class="store-result-item">
                <span class="store-result-type">${escHtml(r.result_type)}</span>
                <strong>${escHtml(r.source_name || r.source_file.split('/').pop())}</strong>
                ${r.field_name ? ` · <span style="color:var(--primary)">${escHtml(r.field_name)}</span>: ${escHtml(r.value)}` : ''}
                ${r.entity_type ? ` · <span style="color:#7c3aed">[${escHtml(r.entity_type)}]</span> ${escHtml(r.value)}` : ''}
                <div style="color:var(--text-secondary);margin-top:0.2rem">${escHtml(truncateText(r.snippet || '', 120))}</div>
            </div>`;
        });
        html += '</div>';
        panel.innerHTML = html;
    } catch (e) {
        panel.innerHTML = `<div class="alert alert-error">搜索失败: ${escHtml(e.message)}</div>`;
    }
}

// ── Store document operations ──────────────────────────────────────────────────

function downloadUrlFor(outputFile) {
    if (!outputFile) return '';
    const name = String(outputFile).split('/').pop();
    return `${API_BASE}/api/download/${encodeURIComponent(name)}`;
}

function _storeStatusDiv() {
    const d = document.getElementById('store-task-status');
    d.style.display = '';
    return d;
}

async function viewStoredDocument(documentId) {
    // Show or create the detail panel below store-content-panel
    let detailPanel = document.getElementById('store-detail-panel');
    if (!detailPanel) {
        detailPanel = document.createElement('div');
        detailPanel.id = 'store-detail-panel';
        const contentPanel = document.getElementById('store-content-panel');
        contentPanel.parentNode.insertBefore(detailPanel, contentPanel.nextSibling);
    }
    detailPanel.style.display = '';
    detailPanel.innerHTML = '<div style="color:var(--text-secondary);font-size:0.85rem;margin-top:0.5rem">加载详情中...</div>';

    try {
        const res = await fetch(`${API_BASE}/api/store/documents/${encodeURIComponent(documentId)}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        renderStoredDocumentDetail(data, detailPanel);
    } catch (e) {
        detailPanel.innerHTML = `<div class="alert alert-error" style="margin-top:0.5rem">详情加载失败: ${escHtml(e.message)}</div>`;
    }
}

function renderStoredDocumentDetail(data, container) {
    const doc = data;
    const textBlocks = data.text_blocks || [];
    const entities = data.entities || [];
    const fields = data.fields || [];
    const qualityIssues = data.quality_issues || [];
    const qTotal = doc.quality_issue_count !== undefined ? doc.quality_issue_count : qualityIssues.length;

    let html = `
        <div style="border:1px solid var(--border);border-radius:8px;padding:1rem;background:#f8fafc;margin-top:0.75rem">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.75rem">
                <div style="font-weight:700;font-size:0.95rem">📄 ${escHtml(doc.title || doc.source_name || '文档详情')}</div>
                <button onclick="document.getElementById('store-detail-panel').style.display='none'"
                    style="font-size:0.8rem;padding:0.25rem 0.6rem;border:1px solid var(--border);border-radius:4px;cursor:pointer;background:#fff">✕ 关闭</button>
            </div>
            <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:0.4rem;margin-bottom:0.75rem;font-size:0.82rem">
                <div><strong>类型：</strong>${escHtml(doc.file_type || '-')}</div>
                <div><strong>来源类型：</strong>${escHtml(doc.source_type || '-')}</div>
                <div><strong>入库时间：</strong>${escHtml((doc.created_at || '').slice(0, 16))}</div>
                <div><strong>文本块：</strong>${doc.text_block_count ?? textBlocks.length}</div>
                <div><strong>表格：</strong>${doc.table_count ?? (data.tables || []).length}</div>
                <div><strong>实体：</strong>${doc.entity_count ?? entities.length}</div>
                <div><strong>字段：</strong>${doc.field_count ?? fields.length}</div>
                <div><strong>质量问题：</strong>${qTotal >= 1000 ? '1000+' : qTotal}</div>
            </div>`;

    if (textBlocks.length > 0) {
        html += `<div style="margin-bottom:0.75rem">
            <div style="font-weight:600;font-size:0.85rem;margin-bottom:0.4rem">文本块预览（前 ${Math.min(textBlocks.length, 5)} 块）</div>`;
        textBlocks.slice(0, 5).forEach((b, i) => {
            const c = b.content || '';
            html += `<div style="font-size:0.8rem;padding:0.3rem 0.5rem;border-left:2px solid var(--primary);margin-bottom:0.3rem;background:#fff">
                <span style="color:var(--text-secondary)">#${i + 1}</span> ${escHtml(c.slice(0, 200))}${c.length > 200 ? '…' : ''}
            </div>`;
        });
        html += '</div>';
    }

    if (entities.length > 0) {
        html += `<div style="margin-bottom:0.75rem">
            <div style="font-weight:600;font-size:0.85rem;margin-bottom:0.4rem">实体预览（前 ${Math.min(entities.length, 20)} 个）</div>
            <div style="display:flex;flex-wrap:wrap;gap:0.3rem">`;
        entities.slice(0, 20).forEach(e => {
            html += `<span style="font-size:0.78rem;padding:0.15rem 0.4rem;border-radius:4px;background:#ede9fe;color:#5b21b6">[${escHtml(e.entity_type || '?')}] ${escHtml(e.entity_text || '')}</span>`;
        });
        html += '</div></div>';
    }

    if (fields.length > 0) {
        html += `<div style="margin-bottom:0.75rem">
            <div style="font-weight:600;font-size:0.85rem;margin-bottom:0.4rem">字段预览（前 ${Math.min(fields.length, 20)} 个）</div>
            <table class="fields-table"><tr><th>字段名</th><th>值</th><th>标准化值</th><th>置信度</th></tr>`;
        fields.slice(0, 20).forEach(f => {
            html += `<tr>
                <td>${escHtml(f.field_name || '')}</td>
                <td>${escHtml(String(f.value || '').slice(0, 60))}</td>
                <td style="color:var(--text-secondary)">${escHtml(String(f.normalized_value || '').slice(0, 40))}</td>
                <td>${f.confidence != null ? (f.confidence * 100).toFixed(0) + '%' : '-'}</td>
            </tr>`;
        });
        html += '</table></div>';
    }

    if (qualityIssues.length > 0) {
        html += `<div>
            <div style="font-weight:600;font-size:0.85rem;margin-bottom:0.4rem">质量问题预览（前 ${Math.min(qualityIssues.length, 20)} / ${qTotal >= 1000 ? '1000+' : qTotal} 条）</div>
            <table class="fields-table"><tr><th>类型</th><th>级别</th><th>字段</th><th>建议</th></tr>`;
        qualityIssues.slice(0, 20).forEach(q => {
            html += `<tr>
                <td>${escHtml(q.issue_type || '')}</td>
                <td style="color:${q.severity === 'high' ? 'var(--danger)' : q.severity === 'medium' ? 'var(--warning)' : 'var(--text-secondary)'}">${escHtml(q.severity || '')}</td>
                <td>${escHtml(q.field_name || '')}</td>
                <td style="font-size:0.78rem">${escHtml(q.suggestion || q.reason || '')}</td>
            </tr>`;
        });
        html += '</table></div>';
    }

    html += '</div>';
    container.innerHTML = html;
}

async function exportStoredDocument(documentId) {
    const statusDiv = _storeStatusDiv();
    statusDiv.style.background = '#f8fafc';
    statusDiv.textContent = '导出中...';
    try {
        const res = await fetch(`${API_BASE}/api/store/documents/${encodeURIComponent(documentId)}/export`, {
            method: 'POST',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        const dlUrl = downloadUrlFor(data.output_file || data.download_url);
        statusDiv.style.background = '#f0fdf4';
        statusDiv.innerHTML = `✅ 导出完成 — <a href="${escHtml(dlUrl)}" target="_blank" style="color:var(--primary)">⬇️ 下载数据包</a>`;
    } catch (e) {
        statusDiv.style.background = '#fee2e2';
        statusDiv.textContent = `导出失败: ${e.message}`;
    }
}

async function checkoutStoredDocument(documentId) {
    if (!confirm('将生成数据包并从库中移除该文档，是否继续？')) return;
    const statusDiv = _storeStatusDiv();
    statusDiv.style.background = '#f8fafc';
    statusDiv.textContent = '出库处理中...';
    try {
        const res = await fetch(`${API_BASE}/api/store/documents/${encodeURIComponent(documentId)}/checkout`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ remove_after_export: true }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        const dlUrl = downloadUrlFor(data.output_file || data.download_url);
        statusDiv.style.background = '#eff6ff';
        statusDiv.innerHTML = `✅ 出库完成，文档已从库中移除 — <a href="${escHtml(dlUrl)}" target="_blank" style="color:var(--primary)">⬇️ 下载数据包</a>`;
        refreshStoreAfterMutation();
    } catch (e) {
        statusDiv.style.background = '#fee2e2';
        statusDiv.textContent = `出库失败: ${e.message}`;
    }
}

async function deleteStoredDocument(documentId) {
    if (!confirm('确认删除该入库文档？此操作只删除数据库记录，不删除原始上传文件。')) return;
    const statusDiv = _storeStatusDiv();
    statusDiv.style.background = '#f8fafc';
    statusDiv.textContent = '删除中...';
    try {
        const res = await fetch(`${API_BASE}/api/store/documents/${encodeURIComponent(documentId)}`, {
            method: 'DELETE',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        statusDiv.style.background = '#f0fdf4';
        statusDiv.textContent = '✅ 文档已从库中删除';
        refreshStoreAfterMutation();
    } catch (e) {
        statusDiv.style.background = '#fee2e2';
        statusDiv.textContent = `删除失败: ${e.message}`;
    }
}

function refreshStoreAfterMutation() {
    const detailPanel = document.getElementById('store-detail-panel');
    if (detailPanel) detailPanel.style.display = 'none';
    loadStoreDocs();
}

// ── Main Process ──────────────────────────────────────────────────────────────
async function startProcess() {
    if (sourceFiles.length === 0 && externalSources.length === 0) {
        alert('请先上传数据源文件或配置外部数据源');
        return;
    }
    if (templateFiles.length === 0) { alert('请先上传模板文件'); return; }

    const requirement = document.getElementById('requirement').value;
    const useLLM = document.getElementById('use-llm').checked;
    const strictMode = document.getElementById('strict-mode').checked;
    const enableQuality = document.getElementById('enable-quality-detection').checked;
    const enableFusion = document.getElementById('enable-data-fusion').checked;
    const storeData = document.getElementById('store-extracted-data').checked;
    const timeBudget = parseInt(document.getElementById('time-budget').value) || 90;

    let fieldAliases = {};
    try {
        const aliasText = document.getElementById('field-aliases').value.trim();
        if (aliasText) fieldAliases = JSON.parse(aliasText);
    } catch (e) { /* ignore bad JSON */ }

    requestStartedWithEmptyRequirement = !requirement.trim();
    currentTaskId = '';
    displayedLogCount = 0;
    seenWarnings = new Set();
    latestTaskSnapshot = null;
    completedResults = [];
    evaluationResults = [];

    updateProcessButton(true, '上传数据源中...');

    const stepsSection = document.getElementById('steps-section');
    stepsSection.style.display = '';
    resetSteps();
    renderTaskOverview({
        task_id: '-',
        stage_message: '准备执行',
        progress: 0,
        template_statuses: templateFiles.map(file => ({
            template_file: file.name, status: 'pending', current_stage: 'pending',
            records_extracted: 0, output_file: '', warnings: [], error: '',
        })),
    });

    const logsSection = document.getElementById('logs-section');
    logsSection.style.display = '';
    document.getElementById('log-container').innerHTML = '';
    addLog('准备上传文件并创建任务...', 'info');

    const resultsSection = document.getElementById('results-section');
    resultsSection.style.display = 'none';
    document.getElementById('results-container').innerHTML = '';
    document.getElementById('auto-req-hint').style.display = 'none';

    // Time budget warning
    const budgetWarning = document.getElementById('time-budget-warning');
    if (budgetWarning) {
        budgetWarning.style.display = timeBudget < 30 ? '' : 'none';
    }

    const hasExternalSources = externalSources.length > 0;

    try {
        // Upload source files
        let sourcePaths = [];
        if (sourceFiles.length > 0) {
            addLog(`上传 ${sourceFiles.length} 个 source 文件中...`, 'info');
            const uploadedSources = await uploadFiles('/api/files/upload', sourceFiles);
            sourcePaths = uploadedSources.map(item => item.path);
            addLog(`数据源上传完成: ${sourcePaths.length} 个`, 'success');
        }

        // Show source contribution panel if multi-source
        if (hasExternalSources) {
            document.getElementById('source-contribution-panel').style.display = '';
            document.getElementById('source-contribution-content').innerHTML =
                `<div style="font-size:0.82rem;color:var(--text-secondary)">已配置 ${sourcePaths.length} 个本地文件 + ${externalSources.length} 个外部数据源，将使用多源模式处理...</div>`;
        }

        let successCount = 0;
        let errorCount = 0;

        for (let index = 0; index < templateFiles.length; index++) {
            const templateFile = templateFiles[index];
            addLog(`上传模板 ${index + 1}/${templateFiles.length}: ${templateFile.name}`, 'info');
            updateProcessButton(true, `模板 ${index + 1}/${templateFiles.length} 上传中...`);

            const uploadedTemplate = await uploadFiles('/api/templates/upload', [templateFile]);
            const templatePath = uploadedTemplate[0]?.path;
            if (!templatePath) throw new Error(`模板上传失败: ${templateFile.name}`);

            const options = {
                use_llm: useLLM,
                strict_mode: strictMode,
                enable_quality_detection: enableQuality,
                enable_data_fusion: enableFusion,
                store_extracted_data: storeData,
                time_budget_seconds: timeBudget,
                field_aliases: Object.keys(fieldAliases).length ? fieldAliases : undefined,
            };

            let task;
            if (hasExternalSources) {
                // Multi-source mode
                task = await createMultisourceTask(sourcePaths, [templatePath], requirement, externalSources, options);
            } else {
                task = await createLocalProcessTask(sourcePaths, [templatePath], requirement, options);
            }

            if (!task.task_id) throw new Error(`任务创建失败: ${templateFile.name}`);

            currentTaskId = task.task_id;
            addLog(`模板任务已创建: ${templateFile.name} -> ${task.task_id}`, 'info');
            handleTaskSnapshot(mergeResultsIntoSnapshot(task, index, templateFiles.length, templateFile.name));

            const finalData = await pollTask(task.task_id, data =>
                handleTaskSnapshot(mergeResultsIntoSnapshot(data, index, templateFiles.length, templateFile.name))
            );
            currentTaskId = '';

            if (finalData.results && finalData.results.length > 0) mergeCompletedResults(finalData.results);

            if (finalData.status === 'completed') {
                successCount++;
                addLog(`模板处理完成: ${templateFile.name}`, 'success');
                evaluationResults.push({
                    task_id: finalData.task_id,
                    template_name: templateFile.name,
                    fill_rate: (finalData.results[0] || {}).fill_rate || 0,
                    response_time: (finalData.finished_at || 0) - (finalData.started_at || 0),
                    meets_minimum: (finalData.results[0] || {}).meets_minimum || false,
                    output_file: (finalData.results[0] || {}).output_file || '',
                    quality_issue_count: ((finalData.results[0] || {}).quality_report || {}).summary?.issue_count || 0,
                });
            } else {
                errorCount++;
                addLog(`模板处理失败: ${templateFile.name} - ${finalData.error || '未知错误'}`, 'error');
            }

            showResults({ ...finalData, results: completedResults.slice() }, false);

            // Update source contribution display
            if (hasExternalSources && finalData.results && finalData.results[0]) {
                renderSourceContribution(finalData.results[0]);
            }
        }

        const summaryStatus = errorCount === 0 ? 'completed' : (successCount > 0 ? 'partial' : 'error');
        renderTaskOverview({
            ...(latestTaskSnapshot || {}),
            task_id: latestTaskSnapshot?.task_id || '-',
            status: summaryStatus,
            stage_message: errorCount === 0 ? `全部 ${successCount} 个模板已完成` : `已完成 ${successCount} 个，失败 ${errorCount} 个`,
            progress: 1,
            results: completedResults.slice(),
            template_statuses: buildTemplateStatusesFromResults(templateFiles, completedResults),
        });

        // Show competition panel
        renderCompetitionPanel(evaluationResults);

        // Refresh dashboard
        setTimeout(() => loadAnalyticsDashboard(), 1500);
    } catch (err) {
        addLog('❌ 处理失败: ' + err.message, 'error');
        if (completedResults.length > 0) showResults({ ...(latestTaskSnapshot || {}), results: completedResults.slice() }, false);
        setStepError(normalizeStage('failed'));
    } finally {
        currentTaskId = '';
        updateProcessButton(false);
    }
}

async function createMultisourceTask(sourcePaths, templatePaths, requirement, sources, options) {
    const res = await fetch(API_BASE + '/api/process/multisource', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            source_files: sourcePaths,
            template_files: templatePaths,
            requirement,
            sources,
            options,
        }),
    });
    if (!res.ok) {
        const errText = await res.text();
        throw new Error(`多源任务创建失败: HTTP ${res.status}: ${errText}`);
    }
    return await res.json();
}

function renderSourceContribution(result) {
    const panel = document.getElementById('source-contribution-panel');
    const content = document.getElementById('source-contribution-content');
    if (!panel || !content) return;
    panel.style.display = '';
    const stats = result.source_stats || [];
    if (stats.length === 0) { content.innerHTML = '<div style="font-size:0.82rem;color:var(--text-secondary)">无来源贡献数据</div>'; return; }
    let html = '';
    stats.forEach(s => {
        const contributed = s.contributed_fields > 0 || s.contributed_records > 0;
        html += `<div style="font-size:0.82rem;padding:0.3rem 0;border-bottom:1px solid var(--border)">
            ${contributed ? '✅' : '⚠️'} <strong>${escHtml((s.source_file || '').split('/').pop())}</strong> (${escHtml(s.source_type || s.file_type || '')})
            — 贡献字段 <strong>${s.contributed_fields}</strong>，贡献行 <strong>${s.contributed_records}</strong>
        </div>`;
    });
    content.innerHTML = html;
}

// ── Evaluation / Processing Summary Panel ──────────────────────────────────────
function renderCompetitionPanel(results) {
    if (!results || results.length === 0) return;
    const panel = document.getElementById('competition-panel');
    const summary = document.getElementById('competition-summary');
    if (!panel || !summary) return;
    panel.style.display = '';

    const avgFillRate = results.reduce((s, r) => s + r.fill_rate, 0) / results.length;
    const avgTime = results.reduce((s, r) => s + r.response_time, 0) / results.length;
    const allPassAccuracy = results.every(r => r.fill_rate >= 80);
    const allPassTime = results.every(r => r.response_time <= 90);

    let html = `
        <div class="analytics-grid">
            <div class="analytics-card">
                <div class="stat-value" style="color:${allPassAccuracy ? 'var(--success)' : 'var(--danger)'}">${avgFillRate.toFixed(1)}%</div>
                <div class="stat-label">平均填充率 (≥80%: ${allPassAccuracy ? '✅' : '❌'})</div>
            </div>
            <div class="analytics-card">
                <div class="stat-value" style="color:${allPassTime ? 'var(--success)' : 'var(--danger)'}">${avgTime.toFixed(1)}s</div>
                <div class="stat-label">平均响应时间 (≤90s: ${allPassTime ? '✅' : '❌'})</div>
            </div>
            <div class="analytics-card">
                <div class="stat-value">${results.filter(r => r.meets_minimum).length}/${results.length}</div>
                <div class="stat-label">模板达到目标阈值</div>
            </div>
        </div>`;

    results.forEach(r => {
        const passAcc = r.fill_rate >= 80;
        const passTime = r.response_time <= 90;
        html += `<div class="competition-row">
            <span><strong>${escHtml(r.template_name)}</strong></span>
            <span>填充率: <span class="${passAcc ? 'competition-pass' : 'competition-fail'}">${r.fill_rate.toFixed(1)}%</span></span>
            <span>耗时: <span class="${passTime ? 'competition-pass' : 'competition-fail'}">${r.response_time.toFixed(1)}s</span></span>
            <span>质量问题: ${r.quality_issue_count}</span>
            <span>${r.meets_minimum ? '<span class="competition-pass">✅ 通过</span>' : '<span class="competition-fail">❌ 未达标</span>'}</span>
            ${r.output_file ? `<a class="download-btn" href="${API_BASE}/api/download/${encodeURIComponent(r.output_file.split('/').pop())}" target="_blank" style="padding:0.2rem 0.6rem;font-size:0.78rem">⬇️</a>` : ''}
        </div>`;
    });

    summary.innerHTML = html;
}

async function compareWithGold() {
    const outputFile = document.getElementById('gold-output-file').value.trim();
    const goldFile = document.getElementById('gold-gold-file').value.trim();
    const resultDiv = document.getElementById('gold-compare-result');
    if (!outputFile || !goldFile) { alert('请填写输出文件和标准答案路径'); return; }

    resultDiv.innerHTML = '<span style="color:var(--text-secondary)">对比中...</span>';
    try {
        const res = await fetch(API_BASE + '/api/evaluate/compare', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ output_file: outputFile, gold_file: goldFile, ignore_empty: true }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        const cellAcc = data.cell_accuracy;
        const rowAcc = data.row_accuracy;
        resultDiv.innerHTML = `
            <div style="padding:0.75rem;border:1px solid var(--border);border-radius:8px;background:#f0fdf4;font-size:0.85rem">
                <div><strong>单元格准确率: </strong><span style="font-size:1.1rem;font-weight:700;color:${cellAcc>=80?'var(--success)':'var(--danger)'}">${cellAcc}%</span>
                    ${data.meets_accuracy_threshold ? ' ✅ 达标' : ' ❌ 未达标（需≥80%）'}</div>
                <div><strong>行准确率: </strong>${rowAcc}% | 匹配单元格: ${data.matched_cells}/${data.total_cells} | 比对行: ${data.compared_rows}</div>
                ${data.mismatch_examples.length ? `<div style="margin-top:0.4rem"><strong>差异示例：</strong>行${data.mismatch_examples[0].row} ${escHtml(data.mismatch_examples[0].column)}: 输出「${escHtml(data.mismatch_examples[0].output)}」vs 标准「${escHtml(data.mismatch_examples[0].gold)}」</div>` : ''}
            </div>`;
    } catch (e) {
        resultDiv.innerHTML = `<span style="color:var(--danger)">对比失败: ${escHtml(e.message)}</span>`;
    }
}

// ── Analytics Dashboard ────────────────────────────────────────────────────────
async function loadAnalyticsDashboard() {
    const panel = document.getElementById('analytics-panel');
    if (!panel) return;
    panel.innerHTML = '<div style="color:var(--text-secondary);font-size:0.85rem">加载中...</div>';

    try {
        const res = await fetch(API_BASE + '/api/analytics/dashboard');
        const data = await res.json();
        renderAnalyticsDashboard(data, panel);
        dashboardSnapshot = data;
    } catch (e) {
        panel.innerHTML = `<div style="color:var(--text-secondary);font-size:0.85rem">看板加载失败: ${escHtml(e.message)}</div>`;
    }
}

function renderAnalyticsDashboard(data, container) {
    const store = data.store || {};
    const qTypeDist = data.quality_type_distribution || {};
    const recentTasks = data.recent_tasks || [];

    let html = `<div class="analytics-grid">
        <div class="analytics-card"><div class="stat-value">${store.document_count || 0}</div><div class="stat-label">入库文档数</div></div>
        <div class="analytics-card"><div class="stat-value">${store.table_count || 0}</div><div class="stat-label">数据表格</div></div>
        <div class="analytics-card"><div class="stat-value">${store.entity_count || 0}</div><div class="stat-label">识别实体</div></div>
        <div class="analytics-card"><div class="stat-value">${store.field_count || 0}</div><div class="stat-label">提取字段</div></div>
        <div class="analytics-card">
            <div class="stat-value" style="color:${data.meets_accuracy_threshold ? 'var(--success)' : 'var(--text-secondary)'}">${data.avg_fill_rate ? data.avg_fill_rate.toFixed(1)+'%' : '-'}</div>
            <div class="stat-label">平均填充率 ${data.meets_accuracy_threshold ? '✅' : ''}</div>
        </div>
        <div class="analytics-card">
            <div class="stat-value" style="color:${data.meets_time_threshold ? 'var(--success)' : 'var(--text-secondary)'}">${data.avg_response_time ? data.avg_response_time.toFixed(1)+'s' : '-'}</div>
            <div class="stat-label">平均响应时间 ${data.meets_time_threshold ? '✅' : ''}</div>
        </div>
        <div class="analytics-card"><div class="stat-value">${store.quality_issue_count || 0}</div><div class="stat-label">质量问题总数</div></div>
    </div>`;

    // Source type distribution
    const srcDist = store.file_type_distribution || store.source_type_distribution || {};
    if (Object.keys(srcDist).length > 0) {
        html += '<div style="margin-bottom:1rem"><div style="font-weight:600;font-size:0.88rem;margin-bottom:0.5rem">数据源类型分布</div>';
        const maxCount = Math.max(...Object.values(srcDist));
        Object.entries(srcDist).forEach(([type, count]) => {
            html += `<div class="bar-chart-row">
                <div class="bar-chart-label">${escHtml(type)}</div>
                <div class="bar-chart-bar"><div class="bar-chart-fill" style="width:${Math.round(count/maxCount*100)}%"></div></div>
                <div class="bar-chart-value">${count}</div>
            </div>`;
        });
        html += '</div>';
    }

    // Quality issue distribution
    if (Object.keys(qTypeDist).length > 0) {
        html += '<div style="margin-bottom:1rem"><div style="font-weight:600;font-size:0.88rem;margin-bottom:0.5rem">数据质量问题分布</div>';
        const maxQ = Math.max(...Object.values(qTypeDist));
        Object.entries(qTypeDist).forEach(([type, count]) => {
            html += `<div class="bar-chart-row">
                <div class="bar-chart-label">${escHtml(type)}</div>
                <div class="bar-chart-bar"><div class="bar-chart-fill" style="width:${Math.round(count/maxQ*100)}%;background:var(--warning)"></div></div>
                <div class="bar-chart-value">${count}</div>
            </div>`;
        });
        html += '</div>';
    }

    // Recent tasks table
    if (recentTasks.length > 0) {
        html += '<div><div style="font-weight:600;font-size:0.88rem;margin-bottom:0.5rem">最近任务</div>';
        html += '<table class="fields-table"><tr><th>任务ID</th><th>填充率</th><th>响应时间</th><th>模板数</th><th>质量问题</th><th>时间</th></tr>';
        recentTasks.slice(0, 10).forEach(t => {
            html += `<tr>
                <td style="font-size:0.78rem">${escHtml(t.task_id)}</td>
                <td style="color:${t.fill_rate>=80?'var(--success)':'var(--danger)'}">${t.fill_rate?.toFixed(1) || '-'}%</td>
                <td style="color:${t.response_time<=90?'var(--success)':'var(--danger)'}">${t.response_time?.toFixed(1) || '-'}s</td>
                <td>${t.template_count || '-'}</td>
                <td>${t.quality_issue_count || 0}</td>
                <td style="font-size:0.75rem">${escHtml((t.created_at || '').slice(0, 16))}</td>
            </tr>`;
        });
        html += '</table></div>';
    }

    if (!html.includes('analytics-card')) {
        html = '<div style="color:var(--text-secondary);font-size:0.85rem">暂无统计数据。完成一次处理任务或入库操作后，数据将自动汇聚到此看板。</div>';
    }

    container.innerHTML = html;
}

// ── Existing functions (unchanged) ──────────────────────────────────────────
async function uploadFiles(endpoint, files) {
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    const res = await fetch(API_BASE + endpoint, { method: 'POST', body: formData });
    if (!res.ok) throw new Error(`上传失败 ${endpoint}: HTTP ${res.status}`);
    const data = await res.json();
    if (!data.files || data.files.length === 0) throw new Error(`上传接口未返回文件路径: ${endpoint}`);
    return data.files;
}

async function createLocalProcessTask(sourcePaths, templatePaths, requirement, options) {
    const res = await fetch(API_BASE + '/api/process/local', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_files: sourcePaths, template_files: templatePaths, requirement, options }),
    });
    if (!res.ok) {
        const errText = await res.text();
        throw new Error(`任务创建失败: HTTP ${res.status}: ${errText}`);
    }
    return await res.json();
}

function mergeCompletedResults(results) {
    results.forEach(result => {
        const index = completedResults.findIndex(item => item.template_file === result.template_file);
        if (index >= 0) completedResults[index] = result;
        else completedResults.push(result);
    });
}

function buildTemplateStatusesFromResults(files, results) {
    return files.map(file => {
        const result = results.find(item => basename(item.template_file) === file.name);
        if (!result) return { template_file: file.name, status: 'pending', current_stage: 'pending', records_extracted: 0, output_file: '', warnings: [], error: '' };
        return {
            template_file: result.template_file || file.name,
            status: result.status === 'completed' ? 'completed' : 'error',
            current_stage: result.status === 'completed' ? 'output' : 'failed',
            records_extracted: result.record_count || 0,
            output_file: result.output_file || '',
            warnings: result.warnings || [],
            error: result.status === 'completed' ? '' : ((result.warnings || [])[0] || '模板处理失败'),
        };
    });
}

function mergeResultsIntoSnapshot(data, templateIndex, templateCount, templateName) {
    const mergedResults = completedResults.slice();
    if (data.results && data.results.length > 0) {
        data.results.forEach(result => {
            const index = mergedResults.findIndex(item => item.template_file === result.template_file);
            if (index >= 0) mergedResults[index] = result;
            else mergedResults.push(result);
        });
    }
    const mergedTemplateStatuses = buildTemplateStatusesFromResults(templateFiles, mergedResults);
    const currentTemplatePath = (data.template_statuses && data.template_statuses[0] && data.template_statuses[0].template_file) || templateName;
    const currentStatusIndex = mergedTemplateStatuses.findIndex(item => basename(item.template_file) === basename(currentTemplatePath));
    if (currentStatusIndex >= 0) {
        mergedTemplateStatuses[currentStatusIndex] = {
            ...mergedTemplateStatuses[currentStatusIndex],
            ...(data.template_statuses && data.template_statuses[0] ? data.template_statuses[0] : {}),
        };
    }
    return {
        ...data,
        stage_message: `模板 ${templateIndex + 1}/${templateCount} · ${templateName} · ${data.stage_message || stageLabel(data.current_stage)}`,
        progress: Math.min(1, (templateIndex + (data.progress || 0)) / templateCount),
        results: mergedResults,
        template_statuses: mergedTemplateStatuses,
    };
}

async function pollTask(taskId, onSnapshot = handleTaskSnapshot) {
    const startedAt = Date.now();
    let consecutiveErrors = 0;
    let lastServerUpdate = 0;
    let lastStallWarningAt = 0;

    while (currentTaskId === taskId) {
        try {
            const res = await fetch(`${API_BASE}/api/status/${encodeURIComponent(taskId)}?_=${Date.now()}`);
            if (!res.ok) throw new Error(`状态查询失败: HTTP ${res.status}`);
            const data = await res.json();
            consecutiveErrors = 0;
            onSnapshot(data);
            if ((data.updated_at || 0) > lastServerUpdate) lastServerUpdate = data.updated_at || 0;
            if (data.status === 'completed' || data.status === 'error') return data;
        } catch (err) {
            consecutiveErrors++;
            addLog(`状态轮询异常 (${consecutiveErrors}/5): ${err.message}`, 'warn');
            if (consecutiveErrors >= 5) throw err;
        }
        if (Date.now() - startedAt > 30 * 60 * 1000) throw new Error('任务执行超过 30 分钟');
        if (lastServerUpdate > 0) {
            const stalledMs = Date.now() - (lastServerUpdate * 1000);
            if (stalledMs > 120 * 1000 && Date.now() - lastStallWarningAt > 30 * 1000) {
                lastStallWarningAt = Date.now();
                addLog(`任务在阶段「${stageLabel(latestTaskSnapshot?.current_stage)}」停留较久，继续等待后端返回...`, 'warn');
            }
            if (stalledMs > 240 * 1000) throw new Error(`任务在阶段「${stageLabel(latestTaskSnapshot?.current_stage)}」停滞超过 240 秒`);
        }
        await wait(1000);
    }
    throw new Error('任务被新的请求替换');
}

function handleTaskSnapshot(data) {
    latestTaskSnapshot = data;
    renderTaskOverview(data);
    syncLogs(data.logs || []);
    syncWarnings(data.warnings || []);
    syncAutoRequirement(data);
    updateStepsBySnapshot(data);
    if (data.results && data.results.length > 0) showResults(data, data.status !== 'completed' && data.status !== 'error');
    const progressText = typeof data.progress === 'number' ? `${Math.round(data.progress * 100)}%` : '处理中';
    const stageText = data.stage_message || stageLabel(data.current_stage);
    if (data.status === 'processing' || data.status === 'queued') {
        updateProcessButton(true, `${progressText} ${truncateText(stageText, 18)}`);
    }
}

// ── Steps UI ───────────────────────────────────────────────────────────────────
function resetSteps() {
    document.querySelectorAll('.step').forEach(el => {
        el.className = 'step';
        el.querySelector('.step-status').textContent = '等待';
    });
}

function updateStepsBySnapshot(data) {
    const stage = normalizeStage(data.current_stage || data.status);
    if (data.status === 'completed') { STEP_ORDER.forEach(step => setStepDone(step)); return; }
    if (data.status === 'error') {
        const failedStage = STEP_ORDER.includes(stage) ? stage : STEP_ORDER[STEP_ORDER.length - 1];
        STEP_ORDER.forEach(step => setStepState(step, 'done', '完成'));
        setStepError(failedStage); return;
    }
    const currentIndex = STEP_ORDER.indexOf(stage);
    STEP_ORDER.forEach((step, index) => {
        if (currentIndex === -1) { setStepState(step, '', '等待'); return; }
        if (index < currentIndex) setStepDone(step);
        else if (index === currentIndex) setStepActive(step);
        else setStepState(step, '', '等待');
    });
}

function setStepActive(name) { setStepState(name, 'active', '处理中'); }
function setStepDone(name) { setStepState(name, 'done', '完成'); }
function setStepError(name) { if (!name) return; setStepState(name, 'error', '失败'); }
function setStepState(name, stateClass, statusText) {
    const el = document.querySelector(`.step[data-step="${name}"]`);
    if (el) { el.className = stateClass ? `step ${stateClass}` : 'step'; el.querySelector('.step-status').textContent = statusText; }
}

// ── Logs ───────────────────────────────────────────────────────────────────────
function addLog(text, level) {
    const container = document.getElementById('log-container');
    if (!container) return;
    const line = document.createElement('div');
    line.className = `log-line ${level || 'info'}`;
    line.textContent = text;
    container.appendChild(line);
    container.scrollTop = container.scrollHeight;
}

function syncLogs(logs) {
    for (let i = displayedLogCount; i < logs.length; i++) addLog(logs[i], guessLogLevel(logs[i]));
    displayedLogCount = Math.max(displayedLogCount, logs.length);
}

function syncWarnings(warnings) {
    warnings.forEach(w => { if (!seenWarnings.has(w)) { seenWarnings.add(w); addLog('⚠️ ' + w, 'warn'); } });
}

function syncAutoRequirement(data) {
    if (!data.auto_requirement) return;
    const reqBox = document.getElementById('requirement');
    if (requestStartedWithEmptyRequirement && reqBox && !reqBox.value.trim()) reqBox.value = data.auto_requirement;
    const autoHint = document.getElementById('auto-req-hint');
    if (autoHint) { autoHint.textContent = '【自动识别】' + data.auto_requirement; autoHint.style.display = ''; }
}

function guessLogLevel(text) {
    if (text.includes('Error') || text.includes('error') || text.includes('失败')) return 'error';
    if (text.includes('Warning') || text.includes('warning') || text.includes('警告') || text.includes('[WARN]')) return 'warn';
    if (text.includes('完成') || text.includes('✓') || text.includes('completed')) return 'success';
    return 'info';
}

function renderTaskOverview(data) {
    const taskIdEl = document.getElementById('task-id');
    const stageEl = document.getElementById('task-stage-text');
    const progressTextEl = document.getElementById('task-progress-text');
    const progressBarEl = document.getElementById('task-progress-bar');
    if (taskIdEl) taskIdEl.textContent = data.task_id || '-';
    if (stageEl) stageEl.textContent = data.stage_message || stageLabel(data.current_stage);
    const progress = Math.max(0, Math.min(100, Math.round((data.progress || 0) * 100)));
    if (progressTextEl) progressTextEl.textContent = `${progress}%`;
    if (progressBarEl) progressBarEl.style.width = `${progress}%`;
    const modelUsageEl = document.getElementById('task-model-usage');
    if (modelUsageEl) modelUsageEl.innerHTML = renderModelUsageHtml(data.model_usage, { includeSources: true, includeFallbacks: true });
    const list = document.getElementById('template-status-list');
    if (list) {
        list.innerHTML = '';
        (data.template_statuses || []).forEach(item => {
            const chip = document.createElement('div');
            const status = item.status || 'pending';
            chip.className = `template-status-chip ${status === 'processing' ? 'processing' : status === 'completed' ? 'completed' : status === 'error' ? 'error' : ''}`;
            const name = item.template_file ? item.template_file.split('/').pop() : '模板';
            const stageText = item.current_stage ? stageLabel(item.current_stage) : '等待';
            const countText = item.records_extracted ? ` · ${item.records_extracted} 条` : '';
            chip.textContent = `${name} · ${stageText}${countText}`;
            list.appendChild(chip);
        });
    }
}

function renderModelUsageHtml(modelUsage, options = {}) {
    if (!modelUsage) return '<div class="model-usage-empty">当前任务尚无模型使用记录</div>';
    const totalCalls = Number(modelUsage.total_calls || 0);
    const successfulCalls = Number(modelUsage.successful_calls || 0);
    const actuallyCalled = !!modelUsage.called && totalCalls > 0;
    const perStage = Object.entries(modelUsage.per_stage || {});
    const perSource = Object.entries(modelUsage.per_source || {});
    const perTemplate = Object.entries(modelUsage.per_template || {});
    const fallbackReasons = (modelUsage.fallback_reasons || []).slice(0, options.includeFallbacks ? 8 : 0);
    const validationErrors = (modelUsage.validation_errors || []).slice(0, 4);
    const missingRequiredCalls = (modelUsage.missing_required_calls || []).slice(0, 4);
    const sampleTrace = modelUsage.sample_trace || {};
    const modelText = actuallyCalled ? escHtml(modelUsage.model || 'qwen2.5:14b') : 'model not used';
    const fallbackStatus = actuallyCalled ? (modelUsage.degraded ? '存在降级/警告' : '正常') : (modelUsage.degraded || fallbackReasons.length ? '未调用并进入降级/回退' : 'model not used');
    return `
        <div class="model-usage-title">模型使用</div>
        <div class="model-usage-grid">
            <div><strong>Provider</strong>：${escHtml(modelUsage.provider || 'ollama')}</div>
            <div><strong>Model</strong>：${modelText}</div>
            <div><strong>本次任务</strong>：${actuallyCalled ? '已实际调用 qwen' : 'model not used'}</div>
            <div><strong>总调用数</strong>：${totalCalls}</div>
            <div><strong>成功调用</strong>：${successfulCalls}</div>
            <div><strong>Fallback</strong>：${escHtml(fallbackStatus)}</div>
            <div><strong>可用性</strong>：${escHtml(modelUsage.availability_status || 'unknown')}</div>
            <div><strong>Trace</strong>：${escHtml(basename(modelUsage.trace_file || '') || '暂无')}</div>
            <div><strong>模板级统计</strong>：${perTemplate.length ? perTemplate.map(([n, c]) => `${basename(n)}(${c})`).join('、') : '暂无'}</div>
        </div>
        <div class="model-usage-section"><strong>阶段调用</strong>：${perStage.length ? perStage.map(([s, c]) => `${stageLabel(s)}(${c})`).join('、') : '暂无'}</div>
        ${options.includeSources ? `<div class="model-usage-section"><strong>按 source</strong>：${perSource.length ? perSource.map(([n, c]) => `${basename(n)}(${c})`).join('、') : '暂无'}</div>` : ''}
        ${sampleTrace && Object.keys(sampleTrace).length ? `<div class="model-usage-section"><strong>样例 Trace</strong>：${escHtml(stageLabel(sampleTrace.stage_name||'unknown'))} / ${escHtml(basename(sampleTrace.source_file||''))} / ${sampleTrace.latency_ms||0}ms</div>` : ''}
        ${missingRequiredCalls.length ? `<div class="model-usage-section"><strong>缺失必经调用</strong>：${missingRequiredCalls.map(i => escHtml(`${stageLabel(i.stage||'unknown')} / ${basename(i.source_file||i.template_file||'')||'任务级'}`)). join('；')}</div>` : ''}
        ${fallbackReasons.length ? `<div class="model-usage-section"><strong>未调用/回退原因</strong>：${fallbackReasons.map(i => escHtml(i)).join('；')}</div>` : ''}
        ${validationErrors.length ? `<div class="model-usage-section"><strong>模型校验</strong>：${validationErrors.map(i => escHtml(i)).join('；')}</div>` : ''}
    `;
}

function basename(path) { if (!path) return ''; return String(path).split('/').pop(); }

function updateProcessButton(disabled, label) {
    const btn = document.getElementById('process-btn');
    if (!btn) return;
    btn.disabled = disabled;
    btn.innerHTML = disabled ? `<span class="spinner"></span> ${escHtml(label || '处理中...')}` : '🚀 开始处理';
}

// ── Results ────────────────────────────────────────────────────────────────────
function showResults(data, partial) {
    const section = document.getElementById('results-section');
    const container = document.getElementById('results-container');
    section.style.display = '';
    container.innerHTML = '';
    if (!data.results || data.results.length === 0) { container.innerHTML = '<div class="alert alert-error">没有生成结果</div>'; return; }

    data.results.forEach((result, idx) => {
        const card = document.createElement('div');
        card.className = 'result-card';
        const rateClass = result.fill_rate >= 70 ? 'high' : result.fill_rate >= 30 ? 'medium' : 'low';
        const templateName = result.template_file ? result.template_file.split('/').pop() : `模板 ${idx + 1}`;
        const outputName = result.output_file ? result.output_file.split('/').pop() : '';
        const completionText = result.status === 'completed' ? (result.meets_minimum === false ? '完成 · 未达目标阈值' : '通过') : '失败';

        let fieldsHtml = '';
        if (result.filled_fields && result.filled_fields.length > 0) {
            const displayFields = result.filled_fields.slice(0, 50);
            fieldsHtml = `<table class="fields-table"><tr><th>字段</th><th>位置</th><th>值</th><th>置信度</th><th>来源</th><th>标准化</th><th>备注</th></tr>${displayFields.map(f => {
                let confDisplay, barWidth, barColor;
                const hasEvidence = Array.isArray(f.evidence) && f.evidence.length > 0;
                if (!hasEvidence && f.value) { confDisplay = '无证据'; barWidth = 0; barColor = '#9ca3af'; }
                else if (f.confidence == null) { confDisplay = '未计算'; barWidth = 0; barColor = '#9ca3af'; }
                else { const pct = Math.round(f.confidence * 100); confDisplay = pct + '%'; barWidth = pct; barColor = f.confidence >= 0.85 ? '#16a34a' : f.confidence >= 0.65 ? '#2563eb' : f.confidence >= 0.45 ? '#d97706' : '#dc2626'; }
                const val = f.value != null ? String(f.value).substring(0, 60) : '';
                const normVal = f.normalized_value != null && f.normalized_value !== f.value ? String(f.normalized_value).substring(0, 40) : '';
                const sourceLabel = [f.source_file ? f.source_file.split('/').pop() : '', f.match_method || ''].filter(Boolean).join(' / ');
                return `<tr>
                    <td>${escHtml(f.field_name)}</td>
                    <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(f.target_location||'')}</td>
                    <td>${escHtml(val)}</td>
                    <td><div class="confidence-bar"><div class="confidence-bar-inner" style="width:${barWidth}%;background:${barColor}"></div></div><span style="font-size:0.75rem;color:${barColor}">${escHtml(confDisplay)}</span></td>
                    <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(sourceLabel||'未标记')}</td>
                    <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(normVal)}</td>
                    <td style="font-size:0.75rem">${escHtml(f.missing_reason||'')}</td>
                </tr>`;
            }).join('')}</table>${result.filled_fields.length > 50 ? `<p style="color:var(--text-secondary);font-size:0.8rem;margin-top:0.5rem">显示前 50 个字段（共 ${result.filled_fields.length} 个）</p>` : ''}`;
        }

        let validationHtml = '';
        if (result.validation_report && result.validation_report.length > 0) {
            validationHtml = `<ul class="validation-list">${result.validation_report.map(v => `<li>${v.passed ? '✅' : '❌'} ${escHtml(v.message)}</li>`).join('')}</ul>`;
        }

        let qualityHtml = '';
        if (result.quality_report && result.quality_report.summary) {
            const summary = result.quality_report.summary || {};
            const issues = (result.quality_report.issues || []).slice(0, 80);
            qualityHtml = `<div class="quality-summary"><strong>质量问题</strong>：${summary.issue_count || 0}${summary.truncated ? `（显示 ${summary.returned_issue_count || issues.length} 条）` : ''} | 影响填表：${summary.affects_fill_count || 0} | 类型：${escHtml(Object.entries(summary.issue_type_distribution || {}).map(([k, v]) => `${k}=${v}`).join('、') || '无')}</div>
            <table class="fields-table"><tr><th>类型</th><th>级别</th><th>字段</th><th>值</th><th>来源</th><th>建议</th></tr>${issues.map(issue => `<tr><td>${escHtml(issue.issue_type||'')}</td><td>${escHtml(issue.severity||'')}</td><td>${escHtml(issue.field_name||'')}</td><td>${escHtml(String(issue.raw_value??'').substring(0,60))}</td><td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(basename(issue.source||'')||issue.location||'')}</td><td>${escHtml(issue.suggestion||issue.reason||'')}</td></tr>`).join('')}</table>`;
        }

        let warningsHtml = '';
        if (result.warnings && result.warnings.length > 0) {
            warningsHtml = `<div style="background:#fef9c3;border:1px solid #fbbf24;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.75rem">${result.warnings.map(w => `<div style="font-size:0.82rem;color:#92400e">⚠️ ${escHtml(w)}</div>`).join('')}</div>`;
        }
        if (result.status === 'completed' && result.meets_minimum === false) {
            warningsHtml += `<div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.75rem"><div style="font-size:0.82rem;color:#1d4ed8">结果文件已生成，但当前模板尚未满足目标阈值（填充率 ${result.fill_rate.toFixed(1)}% vs 要求 ≥80%），请结合验证报告继续排查。</div></div>`;
        }

        let evidenceHtml = '';
        if (result.evidence_report && result.evidence_report.length > 0) {
            evidenceHtml = result.evidence_report.map(e => `<div style="font-size:0.8rem;padding:0.3rem 0;border-bottom:1px solid var(--border)"><strong>${escHtml(e.source_file?e.source_file.split('/').pop():'')}</strong> [${escHtml(e.location||'')}] — ${escHtml(e.match_reason||'')} (${e.confidence==null?'置信度未计算':`置信度: ${(e.confidence*100).toFixed(0)}%`})</div>`).join('');
        }

        let sourceStatsHtml = '';
        if (result.source_stats && result.source_stats.length > 0) {
            sourceStatsHtml = `<div style="margin:0.75rem 0;padding:0.75rem;border:1px solid var(--border);border-radius:8px;background:#f8fafc">${result.source_stats.map(stat => `
                <div style="font-size:0.82rem;padding:0.25rem 0;border-bottom:1px solid #e5e7eb">
                    <strong>${escHtml(stat.source_file?stat.source_file.split('/').pop():'')}</strong>：类型 ${escHtml(stat.file_type||'unknown')}，文本块 ${stat.text_blocks}，表 ${stat.tables}，实体块 ${stat.entity_blocks_detected||0}，模板相关 ${stat.relevant_to_template?'是':'否'}，抽取记录 ${stat.extracted_records}，evidence字段 ${stat.evidence_contribution_fields||0}，value字段 ${stat.value_contribution_fields||stat.contributed_fields||0}，value行 ${stat.row_contribution_records||stat.contributed_records||0}，effective cell delta ${stat.effective_cell_delta||0}，qwen ${stat.qwen_used?`已用(${stat.qwen_call_count||0})`:'未用'}
                    ${stat.warnings&&stat.warnings.length?`<div style="color:#b45309;margin-top:0.2rem">${stat.warnings.map(w=>escHtml(w)).join('；')}</div>`:''}
                </div>`).join('')}</div>`;
        }

        const modelUsageHtml = renderModelUsageHtml(result.model_usage || data.model_usage, { includeSources: true, includeFallbacks: true });

        let metricHtml = '';
        if (result.metric_definitions) {
            metricHtml = `<div style="font-size:0.8rem;color:var(--text-secondary);margin:0.5rem 0 0.75rem 0"><div><strong>指标定义</strong></div><div>record_count：${escHtml(result.metric_definitions.record_count||'')}</div><div>rows_filled：${escHtml(result.metric_definitions.rows_filled||'')}</div><div>fill_rate：${escHtml(result.metric_definitions.fill_rate||'')}</div></div>`;
        }

        card.innerHTML = `
            <div class="result-header">
                <h3>📄 ${escHtml(templateName)}</h3>
                <span class="fill-rate ${rateClass}">${completionText} · 填充率 ${result.fill_rate.toFixed(1)}%</span>
            </div>
            <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:0.75rem">
                填充行数: ${result.rows_filled||0} | 抽取记录: ${result.record_count||0}${result.expected_rows?` / 预估 ${result.expected_rows}`:''} | 字段数: ${(result.filled_fields||[]).length} | ${result.timing?`耗时: ${result.timing.total?result.timing.total.toFixed(1):'?'}s`:''}
            </p>
            ${warningsHtml}${metricHtml}${modelUsageHtml}
            ${partial?'<div class="alert alert-success">当前为处理中预览，最终结果可能继续更新。</div>':''}
            ${outputName?`<a class="download-btn" href="${API_BASE}/api/download/${encodeURIComponent(outputName)}" target="_blank">⬇️ 下载结果文件: ${escHtml(outputName)}</a>`:''}
            ${sourceStatsHtml}
            <div class="tab-row" style="margin-top:1rem">
                <button class="tab-btn active" onclick="switchTab(this, 'fields-${idx}')">字段详情</button>
                <button class="tab-btn" onclick="switchTab(this, 'validation-${idx}')">验证报告</button>
                <button class="tab-btn" onclick="switchTab(this, 'quality-${idx}')">质量识别</button>
                <button class="tab-btn" onclick="switchTab(this, 'evidence-${idx}')">证据来源</button>
            </div>
            <div class="tab-content active" id="fields-${idx}">${fieldsHtml||'<p style="color:var(--text-secondary)">无字段数据</p>'}</div>
            <div class="tab-content" id="validation-${idx}">${validationHtml||'<p style="color:var(--text-secondary)">无验证项</p>'}</div>
            <div class="tab-content" id="quality-${idx}">${qualityHtml||'<p style="color:var(--text-secondary)">无质量问题</p>'}</div>
            <div class="tab-content" id="evidence-${idx}">${evidenceHtml||'<p style="color:var(--text-secondary)">无证据数据</p>'}</div>
        `;
        container.appendChild(card);
    });
}

function switchTab(btn, contentId) {
    const row = btn.parentElement;
    row.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const card = row.parentElement;
    card.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(contentId).classList.add('active');
}

// ── Utility ────────────────────────────────────────────────────────────────────
function escHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function normalizeStage(stage) {
    const mapping = { queued: 'cleanup', cleanup: 'cleanup', parse: 'parse', requirement: 'requirement', template: 'template', retrieve: 'retrieve', extract: 'extract', fill: 'fill', validate: 'validate', output: 'output', completed: 'output', failed: 'output', error: 'output' };
    return mapping[stage] || 'parse';
}

function stageLabel(stage) {
    const labels = { queued: '等待执行', cleanup: '环境清理', parse: '文档解析', requirement: '需求解析', template: '模板解析', retrieve: '证据检索', source_probe: '来源探针', extract: '数据抽取', merge: '歧义合并', fill: '模板填充', validate: '结果验证', output: '输出生成', completed: '处理完成', failed: '处理失败', error: '处理失败', pending: '等待' };
    return labels[stage] || stage || '处理中';
}

function truncateText(text, maxLength) {
    if (!text || text.length <= maxLength) return text || '';
    return text.slice(0, maxLength - 1) + '…';
}

function wait(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
