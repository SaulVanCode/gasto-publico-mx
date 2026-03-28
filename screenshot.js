const puppeteer = require('puppeteer');
const path = require('path');

const files = [
  { html: 'img_tweet2_contratacion.html', png: 'tweet2_contratacion.png' },
  { html: 'img_tweet3_farmaceuticas.html', png: 'tweet3_farmaceuticas.png' },
  { html: 'img_tweet4_concentracion.html', png: 'tweet4_concentracion.png' },
  { html: 'img_tweet5_anomalias.html',     png: 'tweet5_anomalias.png' },
  { html: 'infografia.html',               png: 'infografia_full.png', fullPage: true },
];

(async () => {
  const browser = await puppeteer.launch({ headless: true });

  for (const { html, png, fullPage } of files) {
    const page = await browser.newPage();
    const filePath = path.resolve(__dirname, html);

    if (fullPage) {
      await page.setViewport({ width: 1200, height: 800 });
      await page.goto('file:///' + filePath.replace(/\\/g, '/'), { waitUntil: 'networkidle0' });
      await page.screenshot({ path: path.resolve(__dirname, 'images', png), fullPage: true });
    } else {
      await page.setViewport({ width: 1200, height: 675 });
      await page.goto('file:///' + filePath.replace(/\\/g, '/'), { waitUntil: 'networkidle0' });
      await page.screenshot({ path: path.resolve(__dirname, 'images', png), clip: { x: 0, y: 0, width: 1200, height: 675 } });
    }

    console.log(`OK: ${png}`);
    await page.close();
  }

  await browser.close();
  console.log('Done!');
})();
