// 一键推送本项目到 GitHub 私有仓库（无需 git），使用 Git Data API 生成单一提交。
// 用法：
//   node scripts/push_github.js --dry-run     # 仅列出将推送的文件
//   $env:GH_TOKEN="<带写权限的 token>"; node scripts/push_github.js
const fs = require('fs');
const path = require('path');
const https = require('https');

const OWNER = 'FineStructureConstant07';
const REPO = 'research-kit';
const ROOT = path.resolve(__dirname, '..');
const BRANCH = 'main';

const IGNORE_DIRS = new Set(['vendor', 'vendor_wheels', 'data_store', 'artifacts', '.venv', '__pycache__', '.git', 'logs', 'reports']);
const IGNORE_EXT = new Set(['.pyc', '.pkl', '.parquet', '.csv', '.keras', '.h5', '.log', '.md.tmp']);
const IGNORE_FILES = new Set(['.env', 'secrets.py']);

function walk(dir, base = '') {
  let files = [];
  for (const name of fs.readdirSync(dir)) {
    if (IGNORE_FILES.has(name)) continue;
    if (name.startsWith('_') && !name.startsWith('__')) continue;  // 临时文件（保留 __init__.py）
    const full = path.join(dir, name);
    const rel = base ? base + '/' + name : name;
    const st = fs.statSync(full);
    if (st.isDirectory()) {
      if (IGNORE_DIRS.has(name)) continue;
      files = files.concat(walk(full, rel));
    } else {
      if (IGNORE_EXT.has(path.extname(name))) continue;
      files.push({ rel: rel.replace(/\\/g, '/'), full, size: st.size });
    }
  }
  return files;
}

function api(method, p, body) {
  return new Promise((resolve) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = https.request({
      host: 'api.github.com', path: p, method,
      headers: {
        'Authorization': 'Bearer ' + process.env.GH_TOKEN,
        'User-Agent': 'dsh-push', 'Accept': 'application/vnd.github+json',
        ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {}),
      },
    }, (res) => {
      let b = '';
      res.on('data', (d) => (b += d));
      res.on('end', () => { let d = null; try { d = JSON.parse(b); } catch (e) { d = b; } resolve({ status: res.statusCode, data: d }); });
    });
    req.on('error', (e) => resolve({ status: 0, data: { error: e.message } }));
    if (payload) req.write(payload);
    req.end();
  });
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const files = walk(ROOT).sort((a, b) => a.rel.localeCompare(b.rel));
  const total = files.reduce((s, f) => s + f.size, 0);
  console.log(`共 ${files.length} 个文件，合计 ${(total / 1024).toFixed(1)} KB`);
  if (dryRun) {
    files.forEach((f) => console.log(`  ${f.rel}  (${(f.size / 1024).toFixed(1)} KB)`));
    return;
  }
  if (!process.env.GH_TOKEN) { console.error('缺少 GH_TOKEN 环境变量'); process.exit(2); }

  // 1) 获取当前分支 head（空仓库则先初始化一次提交）
  const refRes = await api('GET', `/repos/${OWNER}/${REPO}/git/ref/heads/${BRANCH}`);
  let headSha = refRes.status === 200 ? refRes.data.object.sha : null;
  if (!headSha) {
    const seed = await api('PUT', `/repos/${OWNER}/${REPO}/contents/README.md`, {
      message: 'init', content: Buffer.from('# research-kit\n').toString('base64'),
    });
    if (seed.status !== 201) { console.error('seed 失败', seed.status, JSON.stringify(seed.data).slice(0, 200)); process.exit(1); }
    const refNow = await api('GET', `/repos/${OWNER}/${REPO}/git/refs/heads/${BRANCH}`);
    headSha = refNow.data.object.sha;
    console.log('已初始化空仓库，head =', headSha);
  }

  // 2) 为每个文件创建 blob
  const treeItems = [];
  for (const f of files) {
    const content = fs.readFileSync(f.full);
    const blob = await api('POST', `/repos/${OWNER}/${REPO}/git/blobs`, { content: content.toString('base64'), encoding: 'base64' });
    if (blob.status !== 201) { console.error('blob 失败', f.rel, blob.status, JSON.stringify(blob.data).slice(0, 200)); process.exit(1); }
    treeItems.push({ path: f.rel, mode: '100644', type: 'blob', sha: blob.data.sha });
    console.log('  blob ok:', f.rel);
  }

  // 3) 创建 tree
  const tree = await api('POST', `/repos/${OWNER}/${REPO}/git/trees`, { base_tree: headSha || undefined, tree: treeItems });
  if (tree.status !== 201) { console.error('tree 失败', tree.status, JSON.stringify(tree.data).slice(0, 300)); process.exit(1); }

  // 4) 创建 commit
  const commitBody = { message: 'init', tree: tree.data.sha };
  if (headSha) commitBody.parents = [headSha];
  const commit = await api('POST', `/repos/${OWNER}/${REPO}/git/commits`, commitBody);
  if (commit.status !== 201) { console.error('commit 失败', commit.status, JSON.stringify(commit.data).slice(0, 300)); process.exit(1); }

  // 5) 更新/创建分支引用
  if (headSha) {
    const upd = await api('PATCH', `/repos/${OWNER}/${REPO}/git/refs/heads/${BRANCH}`, { sha: commit.data.sha, force: false });
    if (upd.status !== 200) { console.error('ref 更新失败', upd.status, JSON.stringify(upd.data).slice(0, 200)); process.exit(1); }
  } else {
    const cre = await api('POST', `/repos/${OWNER}/${REPO}/git/refs`, { ref: `refs/heads/${BRANCH}`, sha: commit.data.sha });
    if (cre.status !== 201) { console.error('ref 创建失败', cre.status, JSON.stringify(cre.data).slice(0, 200)); process.exit(1); }
  }
  console.log('PUSH_DONE', commit.data.html_url || '');
}

main();
