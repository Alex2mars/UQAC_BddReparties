const url = "https://www.d20pfsrd.com/magic/all-spells/"
const fs = require("fs")
const scrapperurl = require("./scrapperurl");
const { MongoClient } = require('mongodb');


async function scrapeAll(browserInstance){
    let browser;
    try{
        browser = await browserInstance;
        //await scraper(browser)
        await scrapperurl(browser)
    }
    catch(err){
        console.log("Could not resolve the browser instance => ", err);
    }
}

function matchEffect(str) {
    let regex = /<b>([\w+\s?]+)<\/b>\s((([\(?\w+\s\)?]+)|(<a.*?>(\w+)<\/a>([\(?\w+\s\)?]+))))/gm;

    let m;
    const effect = { type: "", value : "", link: ''}

    while ((m = regex.exec(str)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
            regex.lastIndex++;
        }

        effect.type = m[1]

        if (m[5] !== undefined){
            effect.link = m[5]
            effect.value = m[6]

            if (m[7] !== undefined) {
                effect.value += m[7]
            }
        }else {
            effect.value = m[3]
        }
    }

    if (effect.type === ''){
        let regex2 = /<b>(\w+)<\/b>\s(<a.*?>(\w+)<\/a>)/gm
        while ((m = regex2.exec(str)) !== null) {
            // This is necessary to avoid infinite loops with zero-width matches
            if (m.index === regex.lastIndex) {
                regex.lastIndex++;
            }

            effect.type = m[1]
            effect.link = m[2]
            effect.value = m[3]
        }
    }

    return effect
}

function matchCasting(str) {
    const regex = /<b>([\w+\s?]+)<\/b>\s((\d+\s\w+)|(.?)+)/gm;
    let m;

    const casting = { type: "", value : ""}

    while ((m = regex.exec(str)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
            regex.lastIndex++;
        }

        casting.type = m[1]

        if (str.indexOf(",") !== -1){
            casting.value = str.split("</b>")[1].split(",").map(item => item.trim())
        }else{
            casting.value =  m[2]
        }
    }

    return casting
}

function matchSchool(str) {

    const regex = /<b>(\w+)<\/b>\s(<a.*?>(\w+)<\/a>)((\s\(?(\w+)\)?)|(\s\((<a.*?>(\w+)<\/a>)\)))?/gm;
    let m;

    const school = { type: "", value : "", link : '', speciality : ""}

    while ((m = regex.exec(str)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
            regex.lastIndex++;
        }

        school.type = m[1]
        school.link = m[2]
        school.value = m[3]

        if(str.indexOf("(<a") !== -1){
            school.speciality = m[9]
        }else{
            school.speciality = m[6]
        }
    }

    return school;
}

async function scraper(browser){
    let page = await browser.newPage();
    console.log(`Navigating to ${url}...`);
    await page.goto(url);

    let scrapedData = [];
    // Wait for the required DOM to be rendered
    async function scrapeCurrentPage(){
        await page.waitForSelector('.article-content');
        // Get the link to all the required books
        let urls = await page.$$eval('.ogn-childpages ul > li', links => {
            // Extract the links from the data
            links = links.map(el => el.querySelector('a').href)
            return links;
        });

        // Loop through each of those links, open a new page instance and get the relevant data from them
        let pagePromise = (link) => new Promise(async(resolve, reject) => {
            let dataObj = {};
            let newPage = await browser.newPage();
            await newPage.goto(link);
            const title = await newPage.$eval("h1", content => {
                return content.innerHTML
            });

            console.info(title)

            let nodes = await newPage.$$eval(".article-content > p", content => {
                return content.map(node => node.innerHTML)
            });

            if (nodes.length === 0){
                nodes = await newPage.$$eval(".article-content div:first-child > div > p", content => {
                    return content.map(node => node.innerHTML)
                });

                let nodes2 = await newPage.$$eval(".article-content > div > p", content => {
                    return content.map(node => node.innerHTML)
                });

                nodes2.forEach(item => nodes.push(item))
            }

            const description = [];

            let desc = false;
            nodes.forEach(node => {
                if (desc){
                    description.push(node)
                }else if (node.indexOf("DESCRIPTION") !== -1){
                    desc = true
                }

            })

            const rowClass = nodes[0]?.split(";")

            if (title === "Adhesive Spittle")
                console.log({ "class": nodes })

            const classesVal = rowClass[1]?.split(',').map(str => {
                return matchLevel(str)
            })

            const school = matchSchool(rowClass[0])
            const casting = nodes[2]?.split("<br>").map(str => {
                return matchCasting(str)
            })
            const effectsVal = nodes[4]?.split("<br>").map(str => {
                if (str.indexOf(";") !== -1) {
                    return str.split(";").map(string => matchEffect(string))
                }
                return matchEffect(str)
            })

            const effects = []

            effectsVal?.forEach(effect => {
                if(Array.isArray(effect)){
                    effect.forEach(obj => effects.push(obj))
                }else {
                    effects.push(effect)
                }
            })

            const classes = []

            classesVal?.forEach(effect => {
                if(Array.isArray(effect)){
                    effect.forEach(obj => classes.push(obj))
                }else {
                    classes.push(effect)
                }
            })

            dataObj["title"] = title
            dataObj["school"] = school
            dataObj["classes"] = classes
            dataObj["casting"] = casting
            dataObj["effects"] = effects
            dataObj["description"] = description

            resolve(dataObj);
            await newPage.close();
        });

        for(let link in urls){
            let currentPageData = await pagePromise(urls[link]);
            scrapedData.push(currentPageData);
        }

        await page.close();
        return scrapedData;
    }

    let data = await scrapeCurrentPage();

    const uri = "mongodb+srv://root:root@cluster0.v4hqy.mongodb.net/scrapping?retryWrites=true&w=majority";
    const client = new MongoClient(uri);

    try {
        await client.connect()
        await client.db('scrapping').collection("spell").deleteMany({})
        await client.db('scrapping').collection("spell").insertMany(data)
    }catch (err){
        console.error(err)
    }finally {
        await client.close()
    }

    return data;
}


const matchLevel = (str) => {
    const regex = /<a.*?>((\w+)(\/(\w+))?)<\/\w+>\s(\d+)/gm;
    let m;
    const level = { class: "", level : ""}

    while ((m = regex.exec(str)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
            regex.lastIndex++;
        }

        if (m[1].indexOf("/") !== -1){
            return [
                { class: m[2], level : parseInt(m[5])},
                { class: m[4], level : parseInt(m[5])}
            ]
        }else{
            level.class = m[2]
            level.level = parseInt(m[5])
        }
    }

    return level
}

module.exports = (browserInstance) => scrapeAll(browserInstance)
