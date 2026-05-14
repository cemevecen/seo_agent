const puppeteer = require('puppeteer');

(async () => {
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  await page.setViewport({ width: 390, height: 844, isMobile: true });
  await page.goto('https://seo-agent-production-de1e.up.railway.app/realtime', { waitUntil: 'networkidle0' });
  await page.screenshot({ path: '/Users/cemevecen/.gemini/antigravity/brain/38e3260a-e7c2-465c-8097-43d03284d54d/.tempmediaStorage/mobile_realtime.png', fullPage: true });
  await browser.close();
  console.log('Mobile screenshot captured: mobile_realtime.png');
})();
