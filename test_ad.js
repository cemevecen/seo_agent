const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  
  page.on('console', msg => console.log('PAGE LOG:', msg.type(), msg.text()));
  page.on('pageerror', error => console.log('PAGE ERROR:', error.message));
  page.on('requestfailed', request => console.log('REQUEST FAILED:', request.url(), request.failure().errorText));

  await page.setViewport({ width: 1440, height: 900 });
  await page.goto('https://projectcontrol.up.railway.app/ad', { waitUntil: 'networkidle0' });
  
  await page.screenshot({ path: 'ad_screenshot.png', fullPage: true });
  
  const html = await page.content();
  fs.writeFileSync('ad_rendered.html', html);
  
  await browser.close();
  console.log('Done!');
})();
