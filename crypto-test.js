const crypto = require('crypto');
const readline = require('readline');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const {fastReadFileLines} = require('./fast-read-file.js');

function delay(t, v) {
    return new Promise(resolve => {
        setTimeout(resolve, t, v);
    })
}

function addCommas(str) {
    var parts = (str + "").split("."),
        main = parts[0],
        len = main.length,
        output = "",
        i = len - 1;
    
    while(i >= 0) {
        output = main.charAt(i) + output;
        if ((len - i) % 3 === 0 && i > 0) {
            output = "," + output;
        }
        --i;
    }
    // put decimal part back
    if (parts.length > 1) {
        output += "." + parts[1];
    }
    return output;
}

// make a unique filename using first several letters of
// the string.  Strings are case sensitive, bucket filenames
// cannot be so it has to be case neutralized while retaining
// uniqueness
function makeBucketKey(str) {
    let piece = str.substr(0,2);
    let filename = [];
    // double up each character, but
    for (let ch of piece) {
        filename.push(ch);
        if (ch >= 'a' && ch <= 'z') {
            filename.push("_")
        } else {
            filename.push(ch);
        }
    }
    return filename.join("").toLowerCase();
} 

// this value times the number of total buckets has to fit in memory
const bucketCacheMax = 3000;

class Bucket {
    constructor(filename, writeToDisk = true) {
        this.items = [];
        this.filename = filename;
        this.cnt = 0;
        this.writeToDisk = writeToDisk;
        
        // We dither the bucketCacheMax so that buckets aren't all trying to write at the same time
        // After they write once (and are thus spread out in time), then they will reset to full cache size
        let dither = Math.floor(Math.random() * bucketCacheMax) + 10;
        if (Math.random() > 0.5) {
            dither = -dither;
        }
        this.bucketCacheMax = bucketCacheMax + dither;
    }
    // add an item to cache, flush to disk if necessary
    async add(item) {
        ++this.cnt;
        this.items.push(item);
        if (this.items.length > this.bucketCacheMax) {
            // the dithered cache size is only used on the first write
            // to spread out the writes.  After that, we want a full cache size
            let priorBucketCacheMax = this.bucketCacheMax;
            this.bucketCacheMax = bucketCacheMax;
            await this.flush();
        }
    }
    // write any cached items to disk
    async flush() {
        if (this.writeToDisk && this.items.length)  {
            let data = this.items.join("\n") + "\n";
            this.items.length = 0;
            if (this.flushPending) {
                throw new Error("Can't call flush() when flush is already in progress");
            }
            
            function flushNow() {
                this.flushPending = true;
                return fsp.appendFile(this.filename, data).finally(() => {
                    this.flushPending = false;
                });
            }
            
            // we write to disk with retry because we once go EBUSY (perhaps from a backup program)
            
            let retryCntr = 0;
            const retryMax = 10;
            const retryDelay = 200;
            const retryBackoff = 200;
            let lastErr;
            
            function flushRetry() {
                if (retryCntr > retryMax) {
                    throw lastErr;
                }
                return flushNow.call(this).catch(err => {
                    lastErr = err;
                    console.log("flushNow error, retrying...", err);
                    return delay(retryDelay + (retryCntr++ * retryBackoff)).then(() => {
                        return flushRetry.call(this);
                    });
                });
            }
            
            return flushRetry.call(this);
        }
        this.items.length = 0;
    }
    
    delete() {
        return fsp.unlink(this.filename);
    }
    
    get size() {
        return this.cnt;
    }
}

class BucketCollection {
    constructor(dir, writeToDisk = true) {
        // map key is bucketID, value is bucket object for that key
        this.buckets = new Map();
        this.dir = dir;
    }
    add(key, data) {
        let bucket = this.buckets.get(key);
        if (!bucket) {
            let filename = path.join(this.dir, key);
            bucket = new Bucket(filename, writeToDisk);
            this.buckets.set(key, bucket);
        }
        return bucket.add(data);
    }
    async flush() {
        // this could perhaps be sped up by doing 4 at a time instead of serially
        for (let bucket of this.buckets.values()) {
            await bucket.flush();
        }
    }
    async delete() {
        // delete all the files associated with the buckets
        for (let bucket of this.buckets.values()) {
            await bucket.delete();
        }
    }
    get size() {
        return this.buckets.size;
    }
    getMaxBucketSize() {
        let max = 0;
        for (let bucket of this.buckets.values()) {
            max = Math.max(max, bucket.size);
        }
        return max;        
    }
    
}

// program options
let numToTry = 100_000;
let writeToDisk = true;
let cleanupBucketFiles = true;
let skipAnalyze = false;
let analyzeOnly = false;

// -nodisk        don't write to disk
// -nocleanup     erase bucket files when done
// -analyzeonly   analyze files in bucket directory only
if (process.argv.length > 2) {
    let args = process.argv.slice(2);
    for (let arg of args) {
        arg = arg.toLowerCase();
        switch(arg) {
            case "-nodisk":
                writeToDisk = false;
                break;
            case "-nocleanup":
                cleanupBucketFiles = false;
                break;
            case "-skipanalyze":
                skipAnalyze = true;
                break;
            case "-analyzeonly":
                analyzeOnly = true;
                break;
            default:
                if (/[^\d,]/.test(arg)) {
                    console.log(`Unknown argument ${arg}`);
                    process.exit(1);
                } else {
                    numToTry = parseInt(arg.replace(/,/g, ""), 10);
                }
        }
    }
}

let bucketDir = path.join(__dirname, "buckets");

let collection = new BucketCollection(bucketDir, writeToDisk);

console.log(`Running ${addCommas(numToTry)} random ids`);

const debugMultiple = 100_000;

async function analyze() {
    let cntr = 0;
    const cntrProgress = 10;
    const cntrProgressN = 10n;
    let buffer = null;
    let times = [];
    
    async function processFile(file) {
        if (cntr !== 0 && cntr % cntrProgress === 0) {
            let sum = 0n;
            for (let i = 0; i < cntrProgress; i++) {
                sum += times[i];
            }
            console.log(`Checking bucket #${cntr}, Average readFileTime = ${sum / cntrProgressN}`);
            times.length = 0;
        }
        ++cntr;
        
        let set = new Set();
        
        let startT = process.hrtime.bigint();
        let buffer = null;
        let result = await fastReadFileLines(file, buffer);
        let data = result.lines;
        
        // keep reusing buffer which may have been made larger since last time
        buffer = result.buffer;
        
        //let data = (await fsp.readFile(file, "utf8")).split("\n");
        let afterReadFileT = process.hrtime.bigint();
        for (const lineData of data) {
            let line = lineData.trim();
            if (line) {
                if (set.has(line)) {
                    console.log(`Found conflict on ${data}`);
                } else {
                    set.add(line);
                }
            }
        }
        let loopT = process.hrtime.bigint();
        let divisor = 1000n;
        let readFileTime = (afterReadFileT - startT) / divisor;
        times.push(readFileTime);
        // console.log(`readFileTime = ${readFileTime}, loopTime = ${(loopT - afterReadFileT) / divisor}`);
        
        /*
        
        let rl = readline.createInterface({input:fs.createReadStream(file), crlfDelay: Infinity});
        for await (const line of rl) {
            let data = line.trim();
            if (data) {
                if (set.has(data)) {
                    console.log(`Found conflict on ${data}`);
                } else {
                    set.add(data);
                }
            }
        }

        */


    }
        
    if (analyzeOnly) {
        let files = await fsp.readdir(bucketDir);
        for (let file of files) {
            let fullPath = path.join(bucketDir, file)
            await processFile(fullPath);
        }
    } else {
        for (let bucket of collection.buckets.values()) {
            await processFile(bucket.filename);
        }
    }
}

async function makeRandoms() {
    let start = Date.now();
    
    if (analyzeOnly) {
        return analyze();
    }
    
    for (let i = 0; i < numToTry; i++) {
        if (i !== 0 && i % debugMultiple === 0) {
            console.log(`Attempt #${addCommas(i)}`);
        }
        const idSeed = crypto.randomBytes(16).toString('base64');
//        const idSeed = crypto.randomBytes(16).toString('base64') + '' + Date.now();
        const orderId = idSeed.toString('base64').replace(/[\/\+\=]/g, '');
        //console.log(orderId);

        let bucketKey = makeBucketKey(orderId);
        await collection.add(bucketKey, orderId);
    }
    console.log(`Total buckets: ${collection.size}, Max bucket size: ${collection.getMaxBucketSize()}`);
    //console.log(`No dups found after ${addCommas(numToTry)} attempts`);
    await collection.flush();
    
    let delta = Date.now() - start;
    console.log(`Run time for creating buckets: ${addCommas(delta)}ms, ${addCommas((delta / numToTry) * 1000)}ms per thousand`);
    
    if (!skipAnalyze) {
        console.log("Analyzing buckets...")
        await analyze();
    }
    if (cleanupBucketFiles) {
        console.log("Cleaning up buckets...")
        await collection.delete();
    }
}

makeRandoms();