const browserObject = require('./browser');
const scrapper = require('./scrapper');

//Start the browser and create a browser instance
let browserInstance = browserObject.startBrowser();

// Pass the browser instance to the scraper controller
scrapper(browserInstance)
