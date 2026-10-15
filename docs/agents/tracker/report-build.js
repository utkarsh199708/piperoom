// Builds the daily report page from report JSON files (reporter.md schema):
//   node report-build.js out.html reports/2026-10-15.json reports/2026-10-14.json ...
// Files are sorted by date, newest first; the newest 7 are kept.
const fs = require('fs');
const path = require('path');
const [out, ...files] = process.argv.slice(2);
if (!out || !files.length) { console.error('usage: node report-build.js out.html report.json...'); process.exit(2); }
const reports = files.map(f => JSON.parse(fs.readFileSync(f, 'utf8')))
  .filter(r => r && r.date).sort((a, b) => b.date.localeCompare(a.date)).slice(0, 7);
const tpl = fs.readFileSync(path.join(__dirname, 'report-template.html'), 'utf8');
fs.writeFileSync(out, tpl.replace('__REPORTS__', JSON.stringify(reports)));
console.log(`${out}: ${reports.length} day(s), newest ${reports[0].date}`);
