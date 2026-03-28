const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const files = [
  { html: 'img_tweet2_contratacion.html', png: 'tweet2_contratacion.png' },
  { html: 'img_tweet3_farmaceuticas.html', png: 'tweet3_farmaceuticas.png' },
  { html: 'img_tweet4_concentracion.html', png: 'tweet4_concentracion.png' },
  { html: 'img_tweet5_anomalias.html',     png: 'tweet5_anomalias.png' },
];

(async () => {
  try {
    console.log('Launching...');
    const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });

    for (const { html, png } of files) {
      const page = await browser.newPage();
      await page.setViewport({ width: 1200, height: 675 });

      const filePath = 'file:///' + path.resolve(__dirname, html).split('\\').join('/');
      console.log('Loading ' + html);
      await page.goto(filePath, { waitUntil: 'networkidle0', timeout: 30000 });

      const outPath = path.resolve(__dirname, 'images', png);
      await page.screenshot({ path: outPath, clip: { x: 0, y: 0, width: 1200, height: 675 } });
      console.log('OK: ' + png);
      await page.close();
    }

    // Full infografia
    const page2 = await browser.newPage();
    await page2.setViewport({ width: 1200, height: 800 });
    const infoPath = 'file:///' + path.resolve(__dirname, 'infografia.html').split('\\').join('/');
    console.log('Loading infografia.html');
    await page2.goto(infoPath, { waitUntil: 'networkidle0', timeout: 30000 });
    await page2.screenshot({ path: path.resolve(__dirname, 'images', 'infografia_full.png'), fullPage: true });
    console.log('OK: infografia_full.png');

    await browser.close();
    console.log('Done!');
  } catch(e) {
    console.error('ERROR:', e.message);
    console.error(e.stack);
  }
})();
