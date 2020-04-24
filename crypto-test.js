'use strict';

const crypto = require('crypto');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const {fastReadFileLines} = require('./fast-read-file.js');
const {makeBucketKey, makeBucketKeyBinary} = require('./bucketkey.js');
const { addCommas } = require('../str-utils');
const { Worker } = require('worker_threads');
const { getLogger } = require('../delay-logger');
const { Bench } = require('../measure');
const BufferPool = require('./buffer-pool.js');
const processArgs = require('../cmd-line-args');

const keyLen = 16;

// create base58 encoder that uses only alpha numerics and leaves out O and 0 which get confused
const base58Encode = require('base-x')('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz').encode;

function delay(t, v) {
    return new Promise(resolve => {
        setTimeout(resolve, t, v);
    })
}

const log = getLogger();

function DEBUG(flag, ...args) {
    if (process.env[flag]) {
        log.now(...args);
    }
}


// this value times the number of total buckets has to fit in memory
const bucketCacheMax = 5000;        // number of keys to cache before writing to disk
const bucketTransactions = [];

class Bucket {
    constructor(filename, writeToDisk = true, binary = false) {
        this.items = [];

        this.binary = binary;
        if (binary) {
            this.buffer = Buffer.allocUnsafe(bucketCacheMax * keyLen);
            this.bufferPos = 0;
            // We dither the initial buffer length to try to spread out the disk writes
            // After they write once then they will reset to full buffer size
            let dither = Math.floor(Math.random() * this.buffer.length);
            // usable buffer starts out at least long enough for 10 keys
            this.bufferUsable = Math.max(dither, 10 * keyLen);
        } else {
            // We dither the bucketCacheMax so that buckets aren't all trying to write at the same time
            // After they write once (and are thus spread out in time), then they will reset to full cache size
            let dither = Math.floor(Math.random() * bucketCacheMax);
            if (Math.random() > 0.5) {
                dither = -dither;
            }
            // cache starts out at least 10 long
            this.bucketCacheMax = Math.max(bucketCacheMax + dither, 10);
        }
        this.filename = filename;
        this.cnt = 0;
        this.writeToDisk = writeToDisk;

        // promise to wait for before writing to the file
        this.waitPromise = Promise.resolve();
        this.flushCnt = 0;

        // file handle stuff
        this.fileHandle = null;
        this.filePos = 0;

        // DEBUG code
        this.transactions = [];

    }

    // DEBUG code
    record(msg) {
        /*
        this.transactions.push(msg);
        if (this.transactions.length > 100) {
            this.transactions.shift();    // remove older item
        }*/
    }

    // same functon as add(), but takes data from a piece of a buffer
    addFromBuffer(buffer, start, len) {
        //  this.record(`Add item ${item}, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`);
        if (this.flushCnt > 0) {
            DEBUG("DEBUG_F3", `flushCnt on add() ${this.flushCnt}, ${this.filename}`);
            console.log(this.transactions);
            throw new Error("add() called before flush() finished")
        }
        ++this.cnt;

        // if data won't fit into the buffer, then flush first
        if (this.bufferPos + len >= this.bufferUsable) {
            return this.flush().then(() => {
                // reset usable buffer to the full length after first flush
                this.bufferUsable = this.buffer.length;
                this.bufferPos += buffer.copy(this.buffer, this.bufferPos, start, start + len);
            });
        } else {
            this.bufferPos += buffer.copy(this.buffer, this.bufferPos, start, start + len);
        }
        return true;
    }

    // add an item to cache, flush to disk if necessary
    // This is a funky function.  Because of event loop scheduling issues, (contention with workers firing new keys at us)
    // we don't want to always return a promise.
    // So, instead, we return either true or a promise and the caller must check and await only if it's a promise
    // This allows all the cached add() calls to be processed entirely synchronously and only the ones that have
    // to call flush will be processed with a promise and await
    add(item) {
        //  this.record(`Add item ${item}, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`);
        if (this.flushCnt > 0) {
            DEBUG("DEBUG_F3", `flushCnt on add() ${this.flushCnt}, ${this.filename}`);
            console.log(this.transactions);
            throw new Error("add() called before flush() finished")
        }
        ++this.cnt;

        this.items.push(item);
        if (this.items.length > this.bucketCacheMax) {
            // the dithered cache size is only used on the first write
            // to spread out the writes.  After that, we want a full cache size
            let priorBucketCacheMax = this.bucketCacheMax;
            this.bucketCacheMax = bucketCacheMax;
            return this.flush();
        }
        return true;
    }
    // write any cached items to disk
    async flush() {

        // this.record(`flush() starting, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`);
        //DEBUG("DEBUG_F3", `flush() ${this.flushCnt}, ${this.filename}`);

        // for binary, we only have the "asyncHandle" strategy
        if (this.binary) {
            try {
                ++this.flushCnt;
                if (!this.fileHandle) {
                    // create new file, open for reading and writing, truncate if exists
                    this.fileHandle = await fsp.open(this.filename, "w+");
                }
                // write out the buffer, straight to the file
                const { bytesWritten } = await this.fileHandle.write(this.buffer, 0, this.bufferPos, this.filePos);
                if (bytesWritten !== this.bufferPos) {
                    throw new Error(`All data not written to file ${this.filename}, specified ${this.bufferPos}, only ${bytesWritten} bytes written`);
                }
                // advance file position to write
                this.filePos += this.bufferPos;
                // reset back to start of our buffer for future add() calls
                this.bufferPos = 0;
            } catch(e) {
                if (this.fileHandle) {
                    await this.fileHandle.close().catch(err => {
                        log.now(err);
                    });
                }
                throw e;
            } finally {
                --this.flushCnt;
            }
            return;
        }

        if (this.writeToDisk && this.items.length)  {
            this.items.push("");                // this will give us a trailing \n which we want
            let data = this.items.join("\n");
            this.items.length = 0;
            if (this.flushCnt > 0) {
                DEBUG("DEBUG_F3", `flushCnt ${this.flushCnt}, ${this.filename}`);
                console.log(this.transactions);
                process.exit(1);
                throw new Error("flush operations waiting");
            }

            // for debugging and performance test purposes,
            //   multiple strategies for writing to the file
            const strategy = "asyncHandle";

            if (strategy === "asyncHandle") {
                // keep file handle open
                try {
                    ++this.flushCnt;
                    if (!this.fileHandle) {
                        // create new file, open for reading and writing, truncate if exists
                        this.fileHandle = await fsp.open(this.filename, "w+");
                    }
                    const { bytesWritten } = await this.fileHandle.write(data, this.filePos);
                    if (bytesWritten !== data.length) {
                        throw new Error(`All data not written to file ${this.filename}, specified ${data.length}, only ${bytesWritten} bytes written`);
                    }
                    this.filePos += data.length;
                } catch(e) {
                    if (this.fileHandle) {
                        await this.fileHandle.close().catch(err => {
                            log.now(err);
                        });
                    }
                    throw e;
                } finally {
                    --this.flushCnt;
                }

            } else if (strategy === "async") {
                ++this.flushCnt;
                return fsp.appendFile(this.filename, data).finally(() => {
                    this.record(`flush() ending, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`)
                    --this.flushCnt;
                });

            } else if (strategy === "sync") {
                ++this.flushCnt;
                try {
                    fs.appendFileSync(this.filename, data);
                    this.record(`flush() ending, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`)
                } finally {
                    --this.flushCnt;
                }

            } else if (strategy === "asyncRetry") {
                if (this.flushCnt > 0) {
                    DEBUG("DEBUG_F3", `flushCnt ${this.flushCnt}, ${this.filename}`);
                    console.log(this.transactions);
                    process.exit(1);
                    throw new Error("flush operations waiting");
                }
                if (this.flushCnt > 3) {
                    throw new Error("Exceeded 3 flush operations waiting");
                }

                function flushNow() {
                    return fsp.appendFile(this.filename, data);
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

                ++this.flushCnt;
                this.waitPromise = this.waitPromise.then(() => {
                    return flushRetry.call(this).finally(() => {
                        --this.flushCnt;
                        this.record(`flush() done, flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`);
                    });
                });
                return this.waitPromise;
            }
        } else {
            this.record(`flush() ending (empty), flushCnt=${this.flushCnt}, bucketCacheMax=${this.bucketCacheMax}`)
        }
        this.items.length = 0;
        return this.waitPromise;
    }

    close() {
        if (this.fileHandle) {
            let fh = this.fileHandle;
            this.fileHandle = null;
            return fh.close();
        } else {
            return Promise.resolve();
        }
    }

    delete() {
        this.record("delete");
        return fsp.unlink(this.filename);
    }

    get size() {
        return this.cnt;
    }
}

class BucketCollection {
    constructor(dirs, writeToDisk = true, binary = false) {
        // map key is bucketID, value is bucket object for that key
        this.buckets = new Map();
        this.dirs = dirs;
        this.dirIndex = 0;
        this.writeToDisk = writeToDisk;
        this.binary = binary;
    }

    // cycles through a list of directories, alternating among them
    // in cases where we're involving multiple disks
    getNextDir() {
        if (this.dirIndex >= this.dirs.length) {
            this.dirIndex = 0;
        }
        return this.dirs[this.dirIndex++];
    }

    // same as add(), but data comes from a piece of a buffer
    addFromBuffer(key, buffer, start, len) {
        let bucket = this.buckets.get(key);
        if (!bucket) {
            let filename = path.join(this.getNextDir(), key);
            bucket = new Bucket(filename, this.writeToDisk, this.binary);
            this.buckets.set(key, bucket);
        }
        return bucket.addFromBuffer(buffer, start, len);
    }

    // note: This returns what bucket.add() returns which will be either true or a promise
    // depending upon whether the bucket had to be flushed
    // data may be string or binary buffer
    add(key, data) {
        let bucket = this.buckets.get(key);
        if (!bucket) {
            let filename = path.join(this.getNextDir(), key);
            bucket = new Bucket(filename, this.writeToDisk, this.binary);
            this.buckets.set(key, bucket);
        }
        return bucket.add(data);
    }
    async flush() {
        // this could perhaps be sped up by doing 4 at a time instead of serially

        // because of multi-tasking issues, we get a static list of buckets to flush
        // before we await any of them
        let buckets = Array.from(this.buckets.values());
        for (let bucket of buckets) {
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

const spec = [
    "-binary", false,
    "-generateWorkers=num", 6,
    "-generateOnly", false,
    "-analyzeOnly", false,
    "-dirs=[dir]", [],
    "-numToBatch=num", 2000,
    "-preserveFiles", false,
    "-nodisk", false,
    "-analyzeParallel", 4
];

// module level configuration variables from command line arguments (with defaults)
let {
    binary: doBinary,
    generateWorkers,
    generateOnly,
    analyzeOnly,
    preserveFiles,
    dirs,
    numToBatch,
    unnamed,
    nodisk,
    analyzeParallel,
} = processArgs(spec);

let numToTry = 100_000;
let collection;
let writeToDisk = !nodisk;

// grab unnamed numeric argument for the numToTry
if (unnamed.length === 1) {
    let arg = unnamed[0];
    if (/[^\d,]/.test(arg)) {
        console.log(`Unknown argument ${arg}`);
        process.exit(1);
    } else {
        numToTry = parseInt(arg.replace(/,/g, ""), 10);
    }
} else if (unnamed.length > 1) {
    console.log(`Unknown arguments ${unnamed}`);
}

if (!analyzeOnly) {
    log.now(`Generating ${addCommas(numToTry)} random ids`);
} else {
    log.now(`Analyzing pre-generated files`);
}
log.now(`  generateWorkers:    ${generateWorkers}`);
log.now(`  numToBatch:         ${numToBatch}`);
log.now(`  analyzeParallel:    ${analyzeParallel}`);
log.now(`  binary:             ${doBinary ? "true": "false"}`);
log.now(`  preserve files:     ${preserveFiles ? "true" : "false"}`);
log.now(`\n`);

// this is the older analyze that only knows how to analyze text, not binary
async function analyze() {
    try {
        let cntr = 0;
        const cntrProgress = 10;
        const cntrProgressN = 10n;
        let buffers = [];
        let times = [];

        function getBuffer() {
            if (buffers.length) {
                return buffers.pop();
            } else {
                return null;
            }
        }

        function putBuffer(buf) {
            buffers.push(buf);
        }

        async function processFile(file) {
            if (cntr !== 0 && cntr % cntrProgress === 0) {
                // log(`Checking bucket #${cntr}, Average readFileTime = ${sum / cntrProgressN}`);
                // log(`Checking bucket #${cntr}`);
                log(`Checking bucket #${cntr}`);
                times.length = 0;
            }
            ++cntr;

            let set = new Set();

            let startT = process.hrtime.bigint();
            let result = await fastReadFileLines(file, getBuffer(), 50 * 1024);
            let data = result.lines;

            // keep reusing buffer which may have been made larger since last time
            /*
            if (!buffer) {
                log.now(`new buffer allocated, size ${result.buffer.length}`);
            } else if (buffer !== result.buffer) {
                log.now(`new buffer allocated, size increased from ${buffer.length} to ${result.buffer.length}`)
            }
            */

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

            // put buffer back into circulation now that we're done with it
            putBuffer(result.buffer);
        }

        if (analyzeOnly) {
            let files = [];
            for (let dir of dirs) {
                let items = await fsp.readdir(dir);
                for (let filename of items) {
                    let fullPath = path.join(dir, filename);
                    files.push(fullPath);
                }
            }
            // FIXME: need to randomize the array so we spread out access across multiple disks
            // FIXME: need to implement concurrent processFile() logic as with actual buckets
            for (let file of files) {
                await processFile(file);
            }
        } else {
            // get a static array of bucketes so it's easier to asynchronously iterate
            const buckets = Array.from(collection.buckets.values());
            let index = 0;
            let inFlightCntr = 0;
            let completionCntr = 0;

            // returns promise when this bucket is done
            function run() {
                return new Promise((resolve, reject) => {

                    function runNext() {
                        if (index < buckets.length) {
                            const bucket = buckets[index++];
                            let file = bucket.fileHandle ? bucket.fileHandle : bucket.filename;
                            ++inFlightCntr;
                            return processFile(file).catch(reject).finally(() => {
                                --inFlightCntr;
                                ++completionCntr;
                                if (bucket.fileHandle) {
                                    return bucket.close();
                                }
                            }).then(runNext);
                        } else {
                            // see if all the files are done or there are still more waiting to finish
                            if (completionCntr === buckets.length) {
                                resolve();
                            }
                        }
                    }

                    // start up the desired number of loops
                    for (let i = 0; i < analyzeParallel; i++) {
                        runNext();
                    }
                });
            }

            await run();
        }
        log.now("---------------------------------\nNo conflicting ids found!\n")
    } finally {
        log.flush();
    }
}

const progressMultiple = 100_000;

async function generateRandoms() {
    let start = Date.now();

    let g1Total = 0n;
    let g2Total = 0n;

    for (let i = 1; i <= numToTry; i++) {
        if (i % progressMultiple === 0) {
            log(`Generating #${addCommas(i)}`);
        }
        // original author's code (which lost random characters)
        // const string = crypto.randomBytes(keyLen).toString('base64') + '' + Date.now();
        // const orderId = Buffer.from(idSeed).toString('base64').replace(/[\/\+\=]/g, '');

        // my first edit to keep all random characters by replacing with chars allowed in a filename
        // const idSeed = crypto.randomBytes(keyLen).toString('base64') + '' + Date.now();
        // const orderId = idSeed.toString('base64').replace(/=/g, '').replace(/\+/g, "-").replace(/\//g, "~");

        // new base58 encoding algorithm, also drops the Date.now() as it just isn't needed
        let t1 = process.hrtime.bigint();
        const orderId = base58Encode(crypto.randomBytes(keyLen));
        const bucketKey = makeBucketKey(orderId);
        let t2 = process.hrtime.bigint();
        // collection.add() can return either a promise or behaves synchronously and returns true
        let ret = collection.add(bucketKey, orderId);
        // if this returns a promise, await it, otherwise it was synchronous
        if (typeof ret.then === "function") {
            await ret;
        }
        let t3 = process.hrtime.bigint();
        g1Total += (t2 - t1);
        g2Total += (t3 - t2);
    }

    // set DEBUG_F1 flag in environment for key and bucket generation time
    DEBUG("DEBUG_F1", `keyGeneration = ${addCommas(g1Total)}, bucketGeneration = ${addCommas(g2Total)}`);

    log.now(`Total buckets: ${collection.size}, Max bucket size: ${collection.getMaxBucketSize()}`);
    await collection.flush();

    let delta = Date.now() - start;
    log.now(`Run time for creating buckets: ${addCommas(delta)}ms, ${addCommas((delta / numToTry) * 1000)}ms per thousand`);
}

async function generateRandomsWorkers() {
    return new Promise((resolve, reject) => {
        let workers = new Set();
        let keysReceived = 0;
        let randomsRemaining = numToTry;
        let randomsPerWorker = Math.ceil(numToTry / generateWorkers);
        let randomsProcessed = 0;

        let t1 = process.hrtime.bigint();
        let bucketProcessTime = 0n;

        const keysToProcess = [];
        let processingNow = 0;
        let maxKeysToProcess = 0;
        let pausedWorkers = [];
        let bufferPool = new BufferPool(0);
        const processTotal = new Bench();
        const processWait = new Bench();

        function finish() {
            // we emptied our queue so tell all the paused workers, we're ready for more
            while (pausedWorkers.length) {
                let worker = pausedWorkers.pop();
                let msg = {event: "batch", data: {}};
                if (doBinary) {
                    // there should always be an available buffer here because
                    // a worker can't get into the pausedWorkers without putting a buffer into the pool
                    // send a buffer back to the worker so it can be recycled
                    // to make sure nothing is copied, we're passing the underlying sharedArrayBuffer
                    // not the nodejs wrapper Buffer
                    msg.data.buffer = bufferPool.get().buffer;
                    if (!msg.data.buffer) {
                        throw new Error("bufferPool was empty");
                    }
                }
                worker.postMessage(msg);
            }
        }

        function checkReentrant() {
            // if already in this loop, ignore this call
            if (processingNow || keysToProcess.length === 0) {
                // DEBUG("DEBUG_F2", `processKeys() re-entrancy blocked`);
                if (keysToProcess.length > maxKeysToProcess) {
                    maxKeysToProcess = keysToProcess.length;
                    DEBUG("DEBUG_F2", `2: maxKeysToProcess increased to ${maxKeysToProcess}`);
                }
                return true;
            }
            return false;
        }

        async function processKeysBinary() {
            // keysToProcess is an array of {buffer, cnt} objects containing binary keys
            if (checkReentrant()) {
                return;
            }
            ++processingNow;
            // keysToProcess is an array of {sharedArrayBuffer, cnt} that came from a workerThread
            processTotal.markBegin();
            while (keysToProcess.length) {
                const {sharedArrayBuffer, cnt} = keysToProcess.pop();
                // put nodejs Buffer wrapper back on it (which just sets things on the prototype)
                let buffer = Buffer.from(sharedArrayBuffer);
                let index = 0;
                for (let i = 0; i < cnt; i++) {
                    // have to calculate the key here since all we have it the raw data
                    let key = makeBucketKeyBinary(buffer, index);
                    let ret = collection.addFromBuffer(key, buffer, index, keyLen);
                    index += keyLen;
                    // ret is an optional promise (for performance reasons)
                    if (typeof ret.then === "function") {
                        processWait.markBegin();            // keep track of time waiting for disk writes
                        await ret;
                        processWait.markEnd();
                    }
                    ++randomsProcessed;
                    if (randomsProcessed % progressMultiple === 0) {
                        log(`Generating #${addCommas(randomsProcessed)}`);
                    }
                }
                // put the buffer back in the pool so it can be used again
                bufferPool.add(buffer);
            }
            processTotal.markEnd();
            if (randomsProcessed === numToTry) {
                processWait.markBegin();            // keep track of time waiting for disk writes
                await collection.flush();
                processWait.markEnd();
                resolve();
                log.now(`Processing Complete`);
                log.now(`  processTotal:  ${processTotal.formatSec(1)}`);
                log.now(`  processTotal:  ${processTotal.formatMs(3)}`);
                log.now(`  processWait:   ${processWait.formatMs(3)}`);
                log.now(`  waiting %:     ${((processWait.nsN / processTotal.nsN) * 100).toFixed(1)}%`);
                return;
            }
            finish();
            --processingNow;
        }

        // We serialize the adding of keys here because we were
        // getting re-entrant problems while buckets were awaiting flush
        // and new worker messages arrived and were being processed
        // This way we serialize all the adding to buckets
        async function processKeys() {
            if (checkReentrant()) {
                return;
            }
            ++processingNow;
            // note that during the await in this loop, new keys may be adding to keysToProcess
            while (keysToProcess.length) {
                if (keysToProcess.length > maxKeysToProcess) {
                    maxKeysToProcess = keysToProcess.length;
                    DEBUG("DEBUG_F2", `1: maxKeysToProcess increased to ${maxKeysToProcess}`);
                }
                // we use .pop() instead of .shift() because it seems like it's probably
                // more efficient to take one off the end rather than the beginning
                // and it does not matter if we process keys in FIFO order
                const [bucketKey, orderId] = keysToProcess.pop();
                let ret = collection.add(bucketKey, orderId);
                // if this returns a promise, await it, otherwise it was synchronous
                // for a run of 100,000,000, not awaiting when we don't have to is 30% faster
                if (typeof ret.then === "function") {
                    await ret;
                }
                ++randomsProcessed;
                if (randomsProcessed % progressMultiple === 0) {
                    log(`Generating #${addCommas(randomsProcessed)}`);
                }
            }
            if (randomsProcessed === numToTry) {
                await collection.flush();
                resolve();
                return;
            }
            finish();
            --processingNow;
        }

        for (let i = 0; i < generateWorkers; i++) {
            let num = Math.min(randomsRemaining, randomsPerWorker);
            let worker = new Worker("./worker.js", {
                workerData: {numToTry: num, numToBatch: numToBatch, workerId: i, doBinary: doBinary, keyLen: keyLen}
            });
            workers.add(worker);
            randomsRemaining -= num;

            let maxTransferTime = 0n;
            let transferCum = 0n;
            worker.on('message', async (msg) => {

                // Design note: these message handlers can be called
                // while other parts of this file are paused at an await
                if (msg.event === "batch") {
                    // keep track of the fact that this worker is now paused, waiting for the next batch message
                    pausedWorkers.push(worker);

                    let data = msg.data;
                    // for non-binary, data is an array of arrays where the inner arrays are [bucketKey, orderId]
                    //    orderId is a base58 encoded string
                    if (doBinary) {
                        // if doBinary is set, then data.sharedArrayBuffer is a sharedArrayBuffer full of packed binary ids
                        //    one after the other, keyLen bytes each
                        //    data.cnt is the number of valid ids in the buffer
                        keysReceived += data.cnt;
                        // put the buffer and cnt into the queue
                        keysToProcess.push(data);
                    } else {
                        keysReceived += data.length;
                        for (let item of data) {
                            keysToProcess.push(item);
                        }
                    }

                    // record how long it took for this data transfer
                    let sendDelta = new Bench(msg.postMessageTime).markEnd().ns;
                    transferCum += sendDelta;
                    if (sendDelta > maxTransferTime) {
                        maxTransferTime = sendDelta;
                    }

                    // kick off processing if it's not already going
                    let processFn = doBinary ? processKeysBinary : processKeys;
                    processFn().catch(err => {
                        log.now(err);
                        process.exit(1);
                    });
                } else if (msg.event === "usage") {
                    let data = msg.data;
                    let sendDelta = new Bench(msg.postMessageTime).markEnd();
                    log.now(`worker ${msg.workerId} finished`);
                    log.now(`   busy percentage:   ${data.busyPercentage.toFixed(1)}%`);
                    log.now(`   measureBusy:       ${Bench.formatNsToSec(data.measureBusy, 1)} sec`);
                    log.now(`   measureTotal:      ${Bench.formatNsToSec(data.measureTotal, 1)} sec`);
                    log.now(`   postMessageTime:   ${sendDelta.formatMs(1)}`);
                    log.now(`   maxTransferTime:   ${Bench.formatNsToMs(maxTransferTime, 1)} ms`);
                    log.now(`   totalTransferTime: ${Bench.formatNsToSec(transferCum, 3)} sec`);
                }

            });

            worker.on('exit', code => {
                console.log(`worker ${i} exited`);
                workers.delete(worker);
                if (workers.size === 0) {
                    log.now("Last worker done");
                }
            });

            worker.on('error', err => {
                console.log(`worker ${i} had error`, err);
                reject(err);
            });

            // start the worker processing
            worker.postMessage({event: "batch"});
        }
    });
}

async function preflight() {
    if (dirs.length === 0) {
        dirs.push(__dirname);
    }
    for (let [i, dir] of dirs.entries()) {
        let d = path.join(dir, "buckets");
        dirs[i] = d;
        try {
            fs.mkdirSync(d);
        } catch(e) {
            if (e.code !== 'EEXIST') {
                throw e;
            }
        }
    }
    collection = new BucketCollection(dirs, writeToDisk, doBinary);
}

async function runIt() {
    await preflight();
    let start = Date.now();

    try {
        if (analyzeOnly) {
            return analyze();
        }

        if (!generateWorkers) {
            await generateRandoms();
        } else {
            await generateRandomsWorkers();
        }

        if (!generateOnly) {
            log.now("Analyzing buckets...")
            await analyze();
        }
        log.now(`Total run time = ${addCommas((Date.now() - start)/1000)}`);
        if (!preserveFiles && writeToDisk) {
            log.now("Cleaning up buckets...");
            await collection.delete();
        }
    } finally {
        // make sure any pending logs get sent
        log.flush();
    }

}

runIt().catch(err => {
    log.now(err);
});
