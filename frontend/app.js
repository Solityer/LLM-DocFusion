/* DocFusion Frontend Application */
const API_BASE = '';
const STEP_ORDER = ['cleanup', 'parse', 'requirement', 'template', 'retrieve', 'extract', 'fill', 'validate', 'output'];

let sourceFiles = [];
let templateFiles = [];
let currentTaskId = '';
let displayedLogCount = 0;
let seenWarnings = new Set();
let requestStartedWithEmptyRequirement = false;
let latestTaskSnapshot = null;
let completedResults = [];

// --- Initialization ---
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
    setupUploadZones();
});

function setupUploadZones() {
    setupDropZone('source-drop', 'source-input', 'source-list', 'source');
    setupDropZone('template-drop', 'template-input', 'template-list', 'template');
}

function setupDropZone(dropId, inputId, listId, type) {
    const drop = document.getElementById(dropId);
    const input = document.getElementById(inputId);
    const list = document.getElementById(listId);

    drop.addEventListener('click', () => input.click());

    drop.addEventListener('dragover', (e) => {
        e.preventDefault();
        drop.classList.add('drag-over');
    });

    drop.addEventListener('dragleave', () => {
        drop.classList.remove('drag-over');
    });

    drop.addEventListener('drop', (e) => {
        e.preventDefault();
        drop.classList.remove('drag-over');
        addFiles(e.dataTransfer.files, type);
    });

    input.addEventListener('change', (e) => {
        addFiles(e.target.files, type);
        input.value = '';
    });
}

function addFiles(fileList, type) {
    const arr = type === 'source' ? sourceFiles : templateFiles;
    const listId = type === 'source' ? 'source-list' : 'template-list';

    for (const f of fileList) {
        // Avoid duplicates
        if (!arr.find(x => x.name === f.name)) {
            arr.push(f);
        }
    }

    renderFileList(arr, listId, type);
}

function renderFileList(files, listId, type) {
    const list = document.getElementById(listId);
    list.innerHTML = '';

    files.forEach((f, i) => {
        const div = document.createElement('div');
        div.className = 'file-item';
        div.innerHTML = `
            <span class="file-name">${f.name}</span>
            <span class="file-size">${formatSize(f.size)}</span>
            <span class="file-remove" onclick="removeFile('${type}', ${i})">✕</span>
        `;
        list.appendChild(div);
    });
}

function removeFile(type, index) {
    if (type === 'source') {
        sourceFiles.splice(index, 1);
        renderFileList(sourceFiles, 'source-list', 'source');
    } else {
        templateFiles.splice(index, 1);
        renderFileList(templateFiles, 'template-list', 'template');
    }
}

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(1) + ' MB';
}

// --- Health Check ---
async function checkHealth() {
    const dot = document.querySelector('.status-dot');
    const info = document.getElementById('model-info');
    dot.className = 'status-dot checking';
    info.textContent = '检查模型状态...';

    try {
        const res = await fetch(API_BASE + '/api/health');
        const data = await res.json();

        if (data.ollama_status === 'ok') {
            dot.className = 'status-dot online';
            info.textContent = `后端可用: ${(data.provider || 'ollama')} / 目标模型 ${data.model}`;
        } else {
            dot.className = 'status-dot offline';
            info.textContent = `模型异常: ${(data.provider || 'ollama')} / ${data.ollama_status}`;
        }
    } catch (e) {
        dot.className = 'status-dot offline';
        info.textContent = '后端连接失败';
    }
}

// --- Process ---
async function startProcess() {
    if (sourceFiles.length === 0) {
        alert('请先上传数据源文件');
        return;
    }
    if (templateFiles.length === 0) {
        alert('请先上传模板文件');
        return;
    }

    const requirement = document.getElementById('requirement').value;
    const useLLM = document.getElementById('use-llm').checked;
    const strictMode = document.getElementById('strict-mode').checked;
    const btn = document.getElementById('process-btn');
    requestStartedWithEmptyRequirement = !requirement.trim();
    currentTaskId = '';
    displayedLogCount = 0;
    seenWarnings = new Set();
    latestTaskSnapshot = null;
    completedResults = [];

    updateProcessButton(true, '上传数据源中...');

    const stepsSection = document.getElementById('steps-section');
    stepsSection.style.display = '';
    resetSteps();
    renderTaskOverview({
        task_id: '-',
        stage_message: '准备按官方流程执行',
        progress: 0,
        template_statuses: templateFiles.map(file => ({
            template_file: file.name,
            status: 'pending',
            current_stage: 'pending',
            records_extracted: 0,
            output_file: '',
            warnings: [],
            error: '',
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

    try {
        addLog('上传全部 source 文件中...', 'info');
        const uploadedSources = await uploadFiles('/api/files/upload', sourceFiles);
        const sourcePaths = uploadedSources.map(item => item.path);
        addLog(`数据源上传完成，共 ${sourcePaths.length} 个。接下来按模板逐个创建异步任务。`, 'success');

        let successCount = 0;
        let errorCount = 0;

        for (let index = 0; index < templateFiles.length; index += 1) {
            const templateFile = templateFiles[index];
            addLog(`上传模板 ${index + 1}/${templateFiles.length}: ${templateFile.name}`, 'info');
            updateProcessButton(true, `模板 ${index + 1}/${templateFiles.length} 上传中...`);

            const uploadedTemplate = await uploadFiles('/api/templates/upload', [templateFile]);
            const templatePath = uploadedTemplate[0]?.path;
            if (!templatePath) {
                throw new Error(`模板上传失败: ${templateFile.name}`);
            }

            const task = await createLocalProcessTask(sourcePaths, [templatePath], requirement, {
                use_llm: useLLM,
                strict_mode: strictMode,
            });
            if (!task.task_id) {
                throw new Error(`模板任务创建失败: ${templateFile.name}`);
            }

            currentTaskId = task.task_id;
            addLog(`模板任务已创建: ${templateFile.name} -> ${task.task_id}`, 'info');
            handleTaskSnapshot(mergeResultsIntoSnapshot(task, index, templateFiles.length, templateFile.name));

            const finalData = await pollTask(task.task_id, data =>
                handleTaskSnapshot(mergeResultsIntoSnapshot(data, index, templateFiles.length, templateFile.name))
            );
            currentTaskId = '';

            if (finalData.results && finalData.results.length > 0) {
                mergeCompletedResults(finalData.results);
            }

            if (finalData.status === 'completed') {
                successCount += 1;
                addLog(`模板处理完成: ${templateFile.name}`, 'success');
            } else {
                errorCount += 1;
                addLog(`模板处理失败: ${templateFile.name} - ${finalData.error || finalData.stage_message || '未知错误'}`, 'error');
            }
            showResults({
                ...finalData,
                results: completedResults.slice(),
            }, false);
        }

        const summaryStatus = errorCount === 0 ? 'completed' : (successCount > 0 ? 'partial' : 'error');
        const summaryMessage = errorCount === 0
            ? `全部 ${successCount} 个模板已完成`
            : `已完成 ${successCount} 个模板，失败 ${errorCount} 个模板`;
        renderTaskOverview({
            ...(latestTaskSnapshot || {}),
            task_id: latestTaskSnapshot?.task_id || '-',
            status: summaryStatus,
            stage_message: summaryMessage,
            progress: 1,
            results: completedResults.slice(),
            template_statuses: buildTemplateStatusesFromResults(templateFiles, completedResults),
        });
    } catch (err) {
        addLog('❌ 处理失败: ' + err.message, 'error');
        if (completedResults.length > 0) {
            showResults({
                ...(latestTaskSnapshot || {}),
                results: completedResults.slice(),
            }, false);
        } else if (latestTaskSnapshot && latestTaskSnapshot.results && latestTaskSnapshot.results.length > 0) {
            showResults(latestTaskSnapshot, false);
        }
        setStepError(normalizeStage('failed'));
    } finally {
        currentTaskId = '';
        updateProcessButton(false);
    }
}

async function uploadFiles(endpoint, files) {
    const formData = new FormData();
    files.forEach(file => {
        formData.append('files', file);
    });
    const res = await fetch(API_BASE + endpoint, {
        method: 'POST',
        body: formData,
    });
    if (!res.ok) {
        throw new Error(`上传失败 ${endpoint}: HTTP ${res.status}`);
    }
    const data = await res.json();
    if (!data.files || data.files.length === 0) {
        throw new Error(`上传接口未返回文件路径: ${endpoint}`);
    }
    return data.files;
}

async function createLocalProcessTask(sourcePaths, templatePaths, requirement, options) {
    const res = await fetch(API_BASE + '/api/process/local', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            source_files: sourcePaths,
            template_files: templatePaths,
            requirement,
            options,
        }),
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
        if (index >= 0) {
            completedResults[index] = result;
        } else {
            completedResults.push(result);
        }
    });
}

function buildTemplateStatusesFromResults(files, results) {
    return files.map(file => {
        const result = results.find(item => basename(item.template_file) === file.name);
        if (!result) {
            return {
                template_file: file.name,
                status: 'pending',
                current_stage: 'pending',
                records_extracted: 0,
                output_file: '',
                warnings: [],
                error: '',
            };
        }
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
            if (index >= 0) {
                mergedResults[index] = result;
            } else {
                mergedResults.push(result);
            }
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
            if (!res.ok) {
                throw new Error(`状态查询失败: HTTP ${res.status}`);
            }
            const data = await res.json();
            consecutiveErrors = 0;
            onSnapshot(data);
            if ((data.updated_at || 0) > lastServerUpdate) {
                lastServerUpdate = data.updated_at || 0;
            }

            if (data.status === 'completed' || data.status === 'error') {
                return data;
            }
        } catch (err) {
            consecutiveErrors += 1;
            addLog(`状态轮询异常 (${consecutiveErrors}/5): ${err.message}`, 'warn');
            if (consecutiveErrors >= 5) {
                throw err;
            }
        }

        if (Date.now() - startedAt > 30 * 60 * 1000) {
            throw new Error('任务执行超过 30 分钟，已停止轮询');
        }
        if (lastServerUpdate > 0) {
            const stalledMs = Date.now() - (lastServerUpdate * 1000);
            if (stalledMs > 120 * 1000 && Date.now() - lastStallWarningAt > 30 * 1000) {
                lastStallWarningAt = Date.now();
                addLog(`任务在阶段「${stageLabel(latestTaskSnapshot?.current_stage)}」停留较久，继续等待后端返回...`, 'warn');
            }
            if (stalledMs > 240 * 1000) {
                throw new Error(`任务在阶段「${stageLabel(latestTaskSnapshot?.current_stage)}」停滞超过 240 秒`);
            }
        }
        await wait(1000);
    }

    throw new Error('任务被新的请求替换，请重新查看最新结果');
}

function handleTaskSnapshot(data) {
    latestTaskSnapshot = data;
    renderTaskOverview(data);
    syncLogs(data.logs || []);
    syncWarnings(data.warnings || []);
    syncAutoRequirement(data);
    updateStepsBySnapshot(data);

    if (data.results && data.results.length > 0) {
        showResults(data, data.status !== 'completed' && data.status !== 'error');
    }

    const progressText = typeof data.progress === 'number' ? `${Math.round(data.progress * 100)}%` : '处理中';
    const stageText = data.stage_message || stageLabel(data.current_stage);
    if (data.status === 'processing' || data.status === 'queued') {
        updateProcessButton(true, `${progressText} ${truncateText(stageText, 18)}`);
    }
}

// --- Steps UI ---
function resetSteps() {
    document.querySelectorAll('.step').forEach(el => {
        el.className = 'step';
        el.querySelector('.step-status').textContent = '等待';
    });
}

function updateStepsBySnapshot(data) {
    const stage = normalizeStage(data.current_stage || data.status);
    if (data.status === 'completed') {
        STEP_ORDER.forEach(step => setStepDone(step));
        return;
    }
    if (data.status === 'error') {
        const failedStage = STEP_ORDER.includes(stage) ? stage : STEP_ORDER[STEP_ORDER.length - 1];
        STEP_ORDER.forEach(step => setStepState(step, 'done', '完成'));
        setStepError(failedStage);
        return;
    }

    const currentIndex = STEP_ORDER.indexOf(stage);
    STEP_ORDER.forEach((step, index) => {
        if (currentIndex === -1) {
            setStepState(step, '', '等待');
            return;
        }
        if (index < currentIndex) {
            setStepDone(step);
        } else if (index === currentIndex) {
            setStepActive(step);
        } else {
            setStepState(step, '', '等待');
        }
    });
}

function setStepActive(name) {
    setStepState(name, 'active', '处理中');
}

function setStepDone(name) {
    setStepState(name, 'done', '完成');
}

function setStepError(name) {
    if (!name) {
        return;
    }
    setStepState(name, 'error', '失败');
}

function setStepState(name, stateClass, statusText) {
    const el = document.querySelector(`.step[data-step="${name}"]`);
    if (el) {
        el.className = stateClass ? `step ${stateClass}` : 'step';
        el.querySelector('.step-status').textContent = statusText;
    }
}

// --- Logs ---
function addLog(text, level) {
    const container = document.getElementById('log-container');
    const line = document.createElement('div');
    line.className = `log-line ${level || 'info'}`;
    line.textContent = text;
    container.appendChild(line);
    container.scrollTop = container.scrollHeight;
}

function syncLogs(logs) {
    for (let index = displayedLogCount; index < logs.length; index += 1) {
        addLog(logs[index], guessLogLevel(logs[index]));
    }
    displayedLogCount = Math.max(displayedLogCount, logs.length);
}

function syncWarnings(warnings) {
    warnings.forEach(warning => {
        if (!seenWarnings.has(warning)) {
            seenWarnings.add(warning);
            addLog('⚠️ ' + warning, 'warn');
        }
    });
}

function syncAutoRequirement(data) {
    if (!data.auto_requirement) {
        return;
    }
    const reqBox = document.getElementById('requirement');
    if (requestStartedWithEmptyRequirement && !reqBox.value.trim()) {
        reqBox.value = data.auto_requirement;
    }
    const autoHint = document.getElementById('auto-req-hint');
    autoHint.textContent = '【自动识别】' + data.auto_requirement;
    autoHint.style.display = '';
}

function guessLogLevel(text) {
    if (text.includes('Error') || text.includes('error') || text.includes('失败')) return 'error';
    if (text.includes('Warning') || text.includes('warning') || text.includes('警告') || text.includes('[WARN]')) return 'warn';
    if (text.includes('完成') || text.includes('✓') || text.includes('completed')) return 'success';
    return 'info';
}

function renderTaskOverview(data) {
    document.getElementById('task-id').textContent = data.task_id || '-';
    document.getElementById('task-stage-text').textContent = data.stage_message || stageLabel(data.current_stage);
    const progress = Math.max(0, Math.min(100, Math.round((data.progress || 0) * 100)));
    document.getElementById('task-progress-text').textContent = `${progress}%`;
    document.getElementById('task-progress-bar').style.width = `${progress}%`;
    document.getElementById('task-model-usage').innerHTML = renderModelUsageHtml(data.model_usage, {
        includeSources: true,
        includeFallbacks: true,
    });

    const list = document.getElementById('template-status-list');
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

function renderModelUsageHtml(modelUsage, options = {}) {
    if (!modelUsage) {
        return '<div class="model-usage-empty">当前任务尚无模型使用记录</div>';
    }
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
    const fallbackStatus = actuallyCalled
        ? (modelUsage.degraded ? '存在降级/警告' : '正常')
        : (modelUsage.degraded || fallbackReasons.length ? '未调用并进入降级/回退' : 'model not used');

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
            <div><strong>模板级统计</strong>：${perTemplate.length ? perTemplate.map(([name, count]) => `${basename(name)}(${count})`).join('、') : '暂无'}</div>
        </div>
        <div class="model-usage-section">
            <strong>阶段调用</strong>：
            ${perStage.length ? perStage.map(([stage, count]) => `${stageLabel(stage)}(${count})`).join('、') : '暂无'}
        </div>
        ${options.includeSources ? `<div class="model-usage-section"><strong>按 source</strong>：${perSource.length ? perSource.map(([name, count]) => `${basename(name)}(${count})`).join('、') : '暂无'}</div>` : ''}
        ${sampleTrace && Object.keys(sampleTrace).length ? `<div class="model-usage-section"><strong>样例 Trace</strong>：${escHtml(stageLabel(sampleTrace.stage_name || 'unknown'))} / ${escHtml(basename(sampleTrace.source_file || ''))} / ${sampleTrace.latency_ms || 0}ms / cache=${sampleTrace.cache_used ? 'yes' : 'no'} / fallback=${sampleTrace.fallback_used ? 'yes' : 'no'}</div>` : ''}
        ${missingRequiredCalls.length ? `<div class="model-usage-section"><strong>缺失必经调用</strong>：${missingRequiredCalls.map(item => escHtml(`${stageLabel(item.stage || 'unknown')} / ${basename(item.source_file || item.template_file || '') || '任务级'}`)).join('；')}</div>` : ''}
        ${fallbackReasons.length ? `<div class="model-usage-section"><strong>未调用/回退原因</strong>：${fallbackReasons.map(item => escHtml(item)).join('；')}</div>` : ''}
        ${validationErrors.length ? `<div class="model-usage-section"><strong>模型校验</strong>：${validationErrors.map(item => escHtml(item)).join('；')}</div>` : ''}
    `;
}

function basename(path) {
    if (!path) return '';
    return String(path).split('/').pop();
}

function updateProcessButton(disabled, label) {
    const btn = document.getElementById('process-btn');
    btn.disabled = disabled;
    btn.innerHTML = disabled
        ? `<span class="spinner"></span> ${escHtml(label || '处理中...')}`
        : '🚀 开始处理';
}

// --- Results ---
function showResults(data, partial) {
    const section = document.getElementById('results-section');
    const container = document.getElementById('results-container');
    section.style.display = '';
    container.innerHTML = '';

    if (!data.results || data.results.length === 0) {
        container.innerHTML = '<div class="alert alert-error">没有生成结果</div>';
        return;
    }

    data.results.forEach((result, idx) => {
        const card = document.createElement('div');
        card.className = 'result-card';

        const rateClass = result.fill_rate >= 70 ? 'high' : result.fill_rate >= 30 ? 'medium' : 'low';
        const templateName = result.template_file ? result.template_file.split('/').pop() : `模板 ${idx + 1}`;
        const outputName = result.output_file ? result.output_file.split('/').pop() : '';
        const completionText = result.status === 'completed'
            ? (result.meets_minimum === false ? '完成 · 未达竞赛阈值' : '通过')
            : '失败';

        let fieldsHtml = '';
        if (result.filled_fields && result.filled_fields.length > 0) {
            const displayFields = result.filled_fields.slice(0, 50);
            fieldsHtml = `
                <table class="fields-table">
                    <tr><th>字段</th><th>位置</th><th>值</th><th>置信度</th><th>来源</th><th>备注</th></tr>
                    ${displayFields.map(f => {
                        let confDisplay, barWidth, barColor;
                        const hasEvidence = Array.isArray(f.evidence) && f.evidence.length > 0;
                        if (!hasEvidence && f.value) {
                            confDisplay = '无证据';
                            barWidth = 0;
                            barColor = '#9ca3af';
                        } else if (f.confidence === null || f.confidence === undefined) {
                            confDisplay = '未计算';
                            barWidth = 0;
                            barColor = '#9ca3af';
                        } else {
                            const pct = Math.round(f.confidence * 100);
                            confDisplay = pct + '%';
                            barWidth = pct;
                            barColor = f.confidence >= 0.85 ? '#16a34a'
                                     : f.confidence >= 0.65 ? '#2563eb'
                                     : f.confidence >= 0.45 ? '#d97706'
                                     : '#dc2626';
                        }
                        const val = f.value != null ? String(f.value).substring(0, 60) : '';
                        const sourceLabel = [
                            f.source_file ? f.source_file.split('/').pop() : '',
                            f.match_method || ''
                        ].filter(Boolean).join(' / ');
                        return `<tr>
                            <td>${escHtml(f.field_name)}</td>
                            <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(f.target_location || '')}</td>
                            <td>${escHtml(val)}</td>
                            <td>
                                <div class="confidence-bar"><div class="confidence-bar-inner" style="width:${barWidth}%;background:${barColor}"></div></div>
                                <span style="font-size:0.75rem;color:${barColor}">${escHtml(confDisplay)}</span>
                            </td>
                            <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(sourceLabel || '未标记')}</td>
                            <td style="font-size:0.75rem">${escHtml(f.missing_reason || '')}</td>
                        </tr>`;
                    }).join('')}
                </table>
                ${result.filled_fields.length > 50 ? `<p style="color:var(--text-secondary);font-size:0.8rem;margin-top:0.5rem">显示前 50 个字段（共 ${result.filled_fields.length} 个）</p>` : ''}
            `;
        }

        let validationHtml = '';
        if (result.validation_report && result.validation_report.length > 0) {
            validationHtml = `
                <ul class="validation-list">
                    ${result.validation_report.map(v =>
                        `<li>${v.passed ? '✅' : '❌'} ${escHtml(v.message)}</li>`
                    ).join('')}
                </ul>`;
        }

        let qualityHtml = '';
        if (result.quality_report && result.quality_report.summary) {
            const summary = result.quality_report.summary || {};
            const issues = (result.quality_report.issues || []).slice(0, 80);
            qualityHtml = `
                <div class="quality-summary">
                    <strong>质量问题</strong>：${summary.issue_count || 0}
                    ${summary.truncated ? `（显示 ${summary.returned_issue_count || issues.length} 条）` : ''}
                    | 影响填表：${summary.affects_fill_count || 0}
                    | 类型：${escHtml(Object.entries(summary.issue_type_distribution || {}).map(([k, v]) => `${k}=${v}`).join('、') || '无')}
                </div>
                <table class="fields-table">
                    <tr><th>类型</th><th>级别</th><th>字段</th><th>值</th><th>来源</th><th>建议</th></tr>
                    ${issues.map(issue => `<tr>
                        <td>${escHtml(issue.issue_type || '')}</td>
                        <td>${escHtml(issue.severity || '')}</td>
                        <td>${escHtml(issue.field_name || '')}</td>
                        <td>${escHtml(String(issue.raw_value ?? '').substring(0, 60))}</td>
                        <td style="font-size:0.75rem;color:var(--text-secondary)">${escHtml(basename(issue.source || '') || issue.location || '')}</td>
                        <td>${escHtml(issue.suggestion || issue.reason || '')}</td>
                    </tr>`).join('')}
                </table>`;
        }

        // Warnings
        let warningsHtml = '';
        if (result.warnings && result.warnings.length > 0) {
            warningsHtml = `<div style="background:#fef9c3;border:1px solid #fbbf24;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.75rem">
                ${result.warnings.map(w => `<div style="font-size:0.82rem;color:#92400e">⚠️ ${escHtml(w)}</div>`).join('')}
            </div>`;
        }
        if (result.status === 'completed' && result.meets_minimum === false) {
            warningsHtml += `<div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.75rem">
                <div style="font-size:0.82rem;color:#1d4ed8">结果文件已生成，但当前模板尚未满足竞赛最低阈值，请结合验证报告继续排查。</div>
            </div>`;
        }

        let evidenceHtml = '';
        if (result.evidence_report && result.evidence_report.length > 0) {
            evidenceHtml = result.evidence_report.map(e =>
                `<div style="font-size:0.8rem;padding:0.3rem 0;border-bottom:1px solid var(--border)">
                    <strong>${escHtml(e.source_file ? e.source_file.split('/').pop() : '')}</strong>
                    [${escHtml(e.location || '')}] — ${escHtml(e.match_reason || '')}
                    (${e.confidence === null || e.confidence === undefined ? '置信度未计算' : `置信度: ${(e.confidence * 100).toFixed(0)}%`})
                </div>`
            ).join('');
        }

        let contributionAuditHtml = '';
        if (result.effective_contribution_audit && result.effective_contribution_audit.effective_cell_delta) {
            const audit = result.effective_contribution_audit;
            const delta = audit.effective_cell_delta || {};
            contributionAuditHtml = `<div style="margin:0.75rem 0;padding:0.75rem;border:1px solid var(--border);border-radius:8px;background:#fff7ed">
                <div style="font-weight:600;margin-bottom:0.35rem">有效贡献审计</div>
                <div style="font-size:0.82rem;color:var(--text-secondary)">
                    baseline vs multisource：cell delta ${escHtml(String(delta.changed_cells || 0))}，
                    row delta ${escHtml(String(delta.changed_rows || 0))}，
                    来源 ${escHtml((delta.sources || []).map(s => s.split('/').pop()).join('、') || '无')}
                </div>
                ${audit.errors && audit.errors.length ? `<div style="color:#b91c1c;margin-top:0.35rem">${audit.errors.map(item => escHtml(item)).join('；')}</div>` : ''}
                ${audit.warnings && audit.warnings.length ? `<div style="color:#b45309;margin-top:0.35rem">${audit.warnings.map(item => escHtml(item)).join('；')}</div>` : ''}
            </div>`;
        }

        let sourceStatsHtml = '';
        if (result.source_stats && result.source_stats.length > 0) {
            sourceStatsHtml = `<div style="margin:0.75rem 0;padding:0.75rem;border:1px solid var(--border);border-radius:8px;background:#f8fafc">
                ${result.source_stats.map(stat => `
                    <div style="font-size:0.82rem;padding:0.25rem 0;border-bottom:1px solid #e5e7eb">
                        <strong>${escHtml(stat.source_file ? stat.source_file.split('/').pop() : '')}</strong>
                        ：类型 ${escHtml(stat.file_type || 'unknown')}，文本块 ${stat.text_blocks}，表 ${stat.tables}，实体块 ${stat.entity_blocks_detected || 0}，模板相关 ${stat.relevant_to_template ? '是' : '否'}${stat.relevance_score ? `(${stat.relevance_score})` : ''}，抽取记录 ${stat.extracted_records}，过滤后记录 ${stat.filtered_records || 0}，evidence字段 ${stat.evidence_contribution_fields || 0}，value字段 ${stat.value_contribution_fields || stat.contributed_fields || 0}，value行 ${stat.row_contribution_records || stat.contributed_records || 0}，effective cell delta ${stat.effective_cell_delta || 0}，qwen ${stat.qwen_used ? `已用(${stat.qwen_call_count || 0})` : '未用'}
                        ${stat.qwen_stages && stat.qwen_stages.length ? `<div style="color:#1d4ed8;margin-top:0.2rem">qwen阶段：${stat.qwen_stages.map(stage => escHtml(stageLabel(stage))).join('、')}</div>` : ''}
                        ${stat.narrative_audit && stat.narrative_audit.raw_narrative_records ? `<div style="color:#475569;margin-top:0.2rem">raw_narrative：total=${escHtml(String(stat.narrative_audit.raw_narrative_records.total || 0))}, rule=${escHtml(String(stat.narrative_audit.raw_narrative_records.rule_records || 0))}, qwen=${escHtml(String(stat.narrative_audit.raw_narrative_records.qwen_records || 0))}, stable=${escHtml(String(stat.narrative_audit.raw_narrative_records.stable_records || 0))}, suspicious=${escHtml(String(stat.narrative_audit.raw_narrative_records.suspicious_records || 0))}</div>` : ''}
                        ${stat.narrative_audit && stat.narrative_audit.post_filter_narrative_records ? `<div style="color:#475569;margin-top:0.2rem">post_filter：remaining=${escHtml(String(stat.narrative_audit.post_filter_narrative_records.remaining_records || 0))}, final=${escHtml(String(stat.narrative_audit.post_filter_narrative_records.final_records || 0))}, dropped=${escHtml(Object.entries(stat.narrative_audit.post_filter_narrative_records.dropped_by_stage || {}).map(([k, v]) => `${k}=${v}`).join(', ') || '无')}</div>` : ''}
                        ${stat.narrative_audit && stat.narrative_audit.merge_outcome ? `<div style="color:#475569;margin-top:0.2rem">merge_outcome：${escHtml(Object.entries(stat.narrative_audit.merge_outcome).map(([k, v]) => `${k}=${v}`).join(', '))}</div>` : ''}
                        ${stat.warnings && stat.warnings.length ? `<div style="color:#b45309;margin-top:0.2rem">${stat.warnings.map(w => escHtml(w)).join('；')}</div>` : ''}
                    </div>
                `).join('')}
            </div>`;
        }

        const modelUsageHtml = renderModelUsageHtml(result.model_usage || data.model_usage, {
            includeSources: true,
            includeFallbacks: true,
        });

        let metricHtml = '';
        if (result.metric_definitions) {
            metricHtml = `<div style="font-size:0.8rem;color:var(--text-secondary);margin:0.5rem 0 0.75rem 0">
                <div><strong>指标定义</strong></div>
                <div>record_count：${escHtml(result.metric_definitions.record_count || '')}</div>
                <div>rows_filled：${escHtml(result.metric_definitions.rows_filled || '')}</div>
                <div>expected_rows：${escHtml(result.metric_definitions.expected_rows || '')}</div>
                <div>fill_rate：${escHtml(result.metric_definitions.fill_rate || '')}</div>
            </div>`;
        }

        card.innerHTML = `
            <div class="result-header">
                <h3>📄 ${escHtml(templateName)}</h3>
                <span class="fill-rate ${rateClass}">
                    ${completionText} · 填充率 ${result.fill_rate.toFixed(1)}%
                </span>
            </div>
            <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:0.75rem">
                填充行数: ${result.rows_filled || 0} |
                抽取记录: ${result.record_count || 0}${result.expected_rows ? ` / 预估 ${result.expected_rows}` : ''} |
                字段数: ${(result.filled_fields || []).length} |
                ${result.timing ? `耗时: ${result.timing.total ? result.timing.total.toFixed(1) : '?'}s` : ''}
            </p>

            ${warningsHtml}
            ${metricHtml}
            ${modelUsageHtml}
            ${partial ? '<div class="alert alert-success">当前为处理中预览，最终结果可能继续更新。</div>' : ''}
            ${outputName ? `<a class="download-btn" href="${API_BASE}/api/download/${encodeURIComponent(outputName)}" target="_blank">⬇️ 下载结果文件: ${escHtml(outputName)}</a>` : ''}
            ${contributionAuditHtml}
            ${sourceStatsHtml}

            <div class="tab-row" style="margin-top:1rem">
                <button class="tab-btn active" onclick="switchTab(this, 'fields-${idx}')">字段详情</button>
                <button class="tab-btn" onclick="switchTab(this, 'validation-${idx}')">验证报告</button>
                <button class="tab-btn" onclick="switchTab(this, 'quality-${idx}')">质量识别</button>
                <button class="tab-btn" onclick="switchTab(this, 'evidence-${idx}')">证据来源</button>
            </div>
            <div class="tab-content active" id="fields-${idx}">${fieldsHtml || '<p style="color:var(--text-secondary)">无字段数据</p>'}</div>
            <div class="tab-content" id="validation-${idx}">${validationHtml || '<p style="color:var(--text-secondary)">无验证项</p>'}</div>
            <div class="tab-content" id="quality-${idx}">${qualityHtml || '<p style="color:var(--text-secondary)">无质量问题</p>'}</div>
            <div class="tab-content" id="evidence-${idx}">${evidenceHtml || '<p style="color:var(--text-secondary)">无证据数据</p>'}</div>
        `;

        container.appendChild(card);
    });
}

function switchTab(btn, contentId) {
    // Deactivate siblings
    const row = btn.parentElement;
    row.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    // Toggle content
    const card = row.parentElement;
    card.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(contentId).classList.add('active');
}

function escHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function normalizeStage(stage) {
    const mapping = {
        queued: 'cleanup',
        cleanup: 'cleanup',
        parse: 'parse',
        requirement: 'requirement',
        template: 'template',
        retrieve: 'retrieve',
        extract: 'extract',
        fill: 'fill',
        validate: 'validate',
        output: 'output',
        completed: 'output',
        failed: 'output',
        error: 'output',
    };
    return mapping[stage] || 'parse';
}

function stageLabel(stage) {
    const labels = {
        queued: '等待执行',
        cleanup: '环境清理',
        parse: '文档解析',
        requirement: '需求解析',
        template: '模板解析',
        retrieve: '证据检索',
        source_probe: '来源探针',
        extract: '数据抽取',
        merge: '歧义合并',
        fill: '模板填充',
        validate: '结果验证',
        output: '输出生成',
        completed: '处理完成',
        failed: '处理失败',
        error: '处理失败',
        pending: '等待',
    };
    return labels[stage] || stage || '处理中';
}

function truncateText(text, maxLength) {
    if (!text || text.length <= maxLength) {
        return text || '';
    }
    return text.slice(0, maxLength - 1) + '…';
}

function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
