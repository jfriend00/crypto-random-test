'use strict';

const crypto = require('crypto');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const {fastReadFileLines} = require('./fast-read-file.js');

// create base58 encoder that uses only alpha numerics and leaves out O and 0 which get confused
const base58Encode = require('base-x')('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz').encode;

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

// logging function to output progress every so often based on timeout
// so you see regular progress, but don't overwhelm the console
// You can call getLogger() multiple times to get separate loggers with
// separate timing and triggers
function getLogger(delay = 1000, skipInitial = true) {
    // args here are passed directly to console.log
    let lastOutputTime = 0;
    let pendingOutput;               // array of arguments for console.log()
    let pendingTimer;                // timer for next output

    const fn = function(...args) {
        if (skipInitial && lastOutputTime === 0) {
            lastOutputTime = Date.now();
        }
        // if we haven't sent anything in awhile, just send this one now
        let now = Date.now();
        if (now - lastOutputTime > delay) {
            console.log(...args);
            if (pendingTimer) {
                clearTimeout(pendingTimer);
                pendingTimer = null;
                pendingOutput = null;
            }
            lastOutputTime = now;
        } else {
            // we did send some logging recently, so queue this
            // this will overwrite any previous output
            // if there's a timer, keep it running
            // if no timer, set a new one, based on lastOutputTime
            pendingOutput = args;
            if (!pendingTimer) {
                pendingTimer = setTimeout(() => {
                    // delay time has past since any output was sent
                    fn.flush();
                }, now - lastOutputTime + delay);
            }
        }
    }

    // clear any pending output
    fn.flush = function() {
        if (pendingTimer) {
            clearTimeout(pendingTimer);
            pendingTimer = null;
        }
        if (pendingOutput) {
            console.log(...pendingOutput);
            pendingOutput = null;
        }
        lastOutputTime = Date.now();
    }

    // flush, then output this log msg right now
    fn.now = function(...args) {
        fn.flush();
        console.log(...args);
    }

    return fn;
}

const log = getLogger();


// make a unique filename using first few letters of
// the string.  Strings are case sensitive, bucket filenames
// cannot be so it has to be case neutralized while retaining
// uniqueness
function makeBucketKey(str) {
    // with base58 algorithm, the first character does not have as much randomness
    // so we pick the 2nd and 3rd characters for the bucket keys and this makes
    // much more even buckets
    let piece = str.substr(1,2);
    let filename = [];
    // double up each character, but
    for (let ch of piece) {
        filename.push(ch);
        if (ch >= 'a' && ch <= 'z') {
            filename.push("_");
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
                    // On a few runs, we got EBUSY errors when flushing
                    // My guess is that this was a backup program contending for access
                    // So, I implemented a backoff retry algorithm to make sure we don't lose
                    // data in the middle of a multi-hour run
                    lastErr = err;
                    log.now("flushNow error, retrying...", err);
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
let numToTry = 100_000;           // run this many iterations if no number passed on command line
let writeToDisk = true;
let cleanupBucketFiles = true;
let skipAnalyze = false;
let analyzeOnly = false;

// -noDisk        don't write to disk
// -noCleanup     erase bucket files when done
// -analyzeOnly   analyze files in bucket directory only
// -skipAnalyze   skip the analysis, just generate the bucket files
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

log.now(`Running ${addCommas(numToTry)} random ids`);

async function analyze() {
    try {
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
                // log(`Checking bucket #${cntr}, Average readFileTime = ${sum / cntrProgressN}`);
                // log(`Checking bucket #${cntr}`);
                log(`Checking bucket #${cntr}`);
                times.length = 0;
            }
            ++cntr;

            let set = new Set();

            let startT = process.hrtime.bigint();
            let result = await fastReadFileLines(file, buffer, 50 * 1024);
            let data = result.lines;

            // keep reusing buffer which may have been made larger since last time
            /*
            if (!buffer) {
                log.now(`new buffer allocated, size ${result.buffer.length}`);
            } else if (buffer !== result.buffer) {
                log.now(`new buffer allocated, size increased from ${buffer.length} to ${result.buffer.length}`)
            }
            */
            buffer = result.buffer;

            //let data = (await fsp.readFile(file, "utf8")).split("\n");
            let afterReadFileT = process.hrtime.bigint();
            for (const lineData of data) {
                let line = lineData.trim();
                if (line) {
                    if (set.has(line)) {
                        log.flush();
                        log.now(`Found conflict on ${data}`);
                        log.now(`Exiting program, bucket files preserved.`)
                        process.exit(1);
                    } else {
                        set.add(line);
                    }
                }
            }
            let loopT = process.hrtime.bigint();
            let divisor = 1000n;
            let readFileTime = (afterReadFileT - startT) / divisor;
            times.push(readFileTime);
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
        log.now("No conflicting ids found.")
    } finally {
        log.flush();
    }
}

async function makeRandoms() {
    try {
        let start = Date.now();

        if (analyzeOnly) {
            return analyze();
        }

        // how often to out
        const progressMultiple = 100_000;

        for (let i = 1; i <= numToTry; i++) {
            if (i % progressMultiple === 0) {
                log(`Generating #${addCommas(i)}`);
            }
            // original author's code (which lost random characters)
            // const string = crypto.randomBytes(16).toString('base64') + '' + Date.now();
            // const orderId = Buffer.from(idSeed).toString('base64').replace(/[\/\+\=]/g, '');

            // my first edit to keep all random characters by replacing with chars allowed in a filename
            // const idSeed = crypto.randomBytes(16).toString('base64') + '' + Date.now();
            // const orderId = idSeed.toString('base64').replace(/=/g, '').replace(/\+/g, "-").replace(/\//g, "~");

            // new base58 encoding algorithm, also drops the Date.now() as it just isn't needed
            const orderId = base58Encode(crypto.randomBytes(16));
            const bucketKey = makeBucketKey(orderId);
            await collection.add(bucketKey, orderId);
        }
        log.now(`Total buckets: ${collection.size}, Max bucket size: ${collection.getMaxBucketSize()}`);
        await collection.flush();

        let delta = Date.now() - start;
        log.now(`Run time for creating buckets: ${addCommas(delta)}ms, ${addCommas((delta / numToTry) * 1000)}ms per thousand`);

        if (!skipAnalyze) {
            log.now("Analyzing buckets...")
            await analyze();
        }
        if (cleanupBucketFiles) {
            log.now("Cleaning up buckets...");
            await collection.delete();
        }
    } finally {
        // make sure any pending logs get sent
        log.flush();
    }
}

makeRandoms();
