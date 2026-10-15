// Builds the tracker page and docs/agents/board.md from data.json: node build.js [data.json] [out.html]
const fs=require('fs');const d=JSON.parse(fs.readFileSync(process.argv[2]||__dirname+'/data.json','utf8'));
fs.writeFileSync(process.argv[3]||'piperoom-tracker.html',fs.readFileSync(__dirname+'/template.html','utf8').replace('__DATA__',JSON.stringify(d)));
const L={done:'done',review:'in review',wip:'in progress',todo:'**NOBODY ON IT**',blocked:'**blocked**',check:'needs your call'};
const pr=a=>a.length?a.map(n=>'#'+n).join(', '):'—';
const rows=d.features.flatMap(f=>f.sub.map(s=>({...s,f})));
let m=`# Feature board: who is building what\n\nRules: \`docs/agents/org.md\`. Updated by \`[LEAD-ARCH]\` only. **Snapshot: ${d.asOf}** (task files + every PR on GitHub).\n\n`;
m+=`## Needs an owner, unblocking or a decision\n\n| Part | Agent | What | PR | Status | Note |\n|---|---|---|---|---|---|\n`;
rows.filter(r=>r.s!=='done'&&r.s!=='review').forEach(r=>m+=`| ${r.id} | ${r.agent} | ${r.what} | ${pr(r.prs)} | ${L[r.s]} | ${r.note||''} |\n`);
m+=`\n## In review now\n\n| Part | Agent | What | PR |\n|---|---|---|---|\n`;
rows.filter(r=>r.s==='review').forEach(r=>m+=`| ${r.id} | ${r.agent} | ${r.what} | ${pr(r.prs)} |\n`);
m+=`\n## By feature\n`;
d.features.forEach(f=>{const dn=f.sub.filter(s=>s.s==='done').length;m+=`\n### ${f.id} — ${f.name} (${dn}/${f.sub.length} done)\n\n| Part | Agent | What | PR | Status |\n|---|---|---|---|---|\n`;f.sub.forEach(s=>m+=`| ${s.id} | ${s.agent} | ${s.what} | ${pr(s.prs)} | ${L[s.s]} |\n`);});
fs.writeFileSync(__dirname+'/../board.md',m);
