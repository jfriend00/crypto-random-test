"use strict";
const processArgs = require("../cmd-line-args");
const { fastReadFile,  fastReadFileLines, parseBufferIntoLines } = require('./fast-read-file.js');
const {fs, fsp, fsc, path} = require('../fs-common');
const { addCommas } = require('../str-utils');
const { getLogger } = require('../delay-logger');
const { mapConcurrent, Deferred } = require('../async-utils');
const { Worker } = require('worker_threads');
const { Bench } = require('../measure');

const log = getLogger();

// how long each binary key is
const keyLen = 16;

class WorkerList {
    constructor() {
        this.workers = [];
        this.deferredQueue = [];
    }
    add(worker) {
        this.workers.push(worker);

        // if someone is waiting for a worker,
        // pull the oldest worker out of the list and
        // give it to the oldest deferred that is waiting
        while (this.deferredQueue.length && this.workers.length) {
            let d = this.deferredQueue.shift();
            d.resolve(this.workers.shift());
        }
    }
    // if there's a worker, get one immediately
    // if not, return a promise that resolves with a worker
    //    when next one is available
    get() {
        if (this.workers.length) {
            return Promise.resolve(this.workers.shift());
        } else {
            let d = new Deferred();
            this.deferredQueue.push(d);
            return d.promise;
        }
    }
}

// command line arguments specification for processArgs()
const commandLineSpec = [
    "-numToRead=num", 0,
    "-numConcurrent=num", 4,
    "-numWorkers=num", 5,
    "-skipParsing", false,
    "-preOpenFiles", false,
    "-dir=dir", "./buckets",
    "-binary", false,
];

async function analyzeWithWorkers(options) {

    // get our options and combine with default values
    let {
        numToRead = 0,
        numConcurrent = 4,
        numWorkers = 5,
        skipParsing = false,
        preOpenFiles = false,
        dir: sourceDir = "./buckets",
        binary: doBinary = false,
        files = null,
    } = options;


    if (!files) {
        log.now(`Collecting files from ${sourceDir}`);
        files = await fsc.listDirectory(sourceDir, {type: "files"});
        if (numToRead !== 0) {
            files.length = numToRead;
        }
        if (preOpenFiles) {
            files = await Promise.all(files.map(filename => {
              return fsc.open(filename, "r").then(handle => {
                  handle.filename = filename;
                  return handle;
              });
            }));
        }
    }

    let start = Date.now();
    let totalBytes = 0;

    function calcRate() {
        let now = Date.now();
        let deltaSeconds = (now - start) / 1000;
        return ((totalBytes / deltaSeconds) / (1024 * 1024)).toFixed(1);
    }

    let buffers = [];
    let numRead = 0;

    if (numToRead !== 0) {
        files.length = numToRead;
    }

    let x = process.env["UV_THREADPOOL_SIZE"];
    let libuvThreads = x ? x : 4;

    let msg = `Running:
    number of files: ${files.length}
    number concurrent: ${numConcurrent}
    libuv threads: ${libuvThreads}`;

    log.now(msg);

    const workers = new Set();
    const freeWorkers = new WorkerList();

    let longestWait = 0;
    let totalWait = 0n;

    // this buffer
    async function checkBuffer(buffer, dataLength) {
        let wait = new Bench();
        wait.markBegin();
        let worker = await freeWorkers.get();
        wait.markEnd();
        longestWait = Math.max(longestWait, wait.ms);
        totalWait += wait.ns;

        // return promise that resolves when work generates a result
        // the code that handles a resolved/rejected promise
        // needs to put the Buffer back into the available list
        return new Promise((resolve, reject) => {

            function msgHandler(msg) {
                if (msg.event === "result") {
                    cleanup();
                    freeWorkers.add(worker);
                    resolve(msg.data);
                }
            }

            function errorHandler(err) {
                cleanup();
                reject(err);
            }

            function cleanup() {
                worker.off("error", errorHandler);
                worker.off("message", msgHandler);
            }

            // listen for when this worker is has our result
            worker.on("message", msgHandler);
            worker.on("error", errorHandler);

            if (!buffer.sharedArrayBuffer) {
                throw new TypeError("buffer in checkBuffer() must have a .sharedArrayBuffer property");
            }

            // we are passing a sharedArrayBuffer which will be "shared"
            // between threads and not copied.  The buffer has been removed from the buffers array
            // so no other code should be using it while this worker is using it
            worker.postMessage({
                event: "checkBuffer",
                data: {
                    sharedArrayBuffer: buffer.sharedArrayBuffer,
                    dataLength: dataLength
                }
            });
        });
    }

    // startup the workers
    for (let i = 0; i < numWorkers; i++) {
        let worker = new Worker("./read-speed-worker.js", {
            workerData: {workerId: i, doBinary, keyLen}
        });
        workers.add(worker);
        freeWorkers.add(worker);

        worker.on('exit', code => {
            console.log(`Worker ${i} exited`);
            workers.delete(worker);
            if (workers.size === 0) {
                log.now("Last worker done");
            }
        });

        worker.on('error', err => {
            console.log(`worker ${i} had error`, err);
            process.exit(1);
        });

        // start the worker processing
        //worker.postMessage({event: "batch"});
    }

    let readBench = new Bench();
//    let parseBench = new Bench();

    await mapConcurrent(files, numConcurrent, async (file, index) => {
        // get a buffer from the cache
        let inputBuffer = buffers.length ? buffers.pop() : null;
        let inputBufferLength = inputBuffer ? inputBuffer.length : 0;

        readBench.markBegin();
        let {buffer, bytesRead} = await fastReadFile(file, inputBuffer, 100 * 1024);
        readBench.markEnd();

        if (inputBufferLength  !== buffer.length) {
            log.now(`Buffer grown to ${addCommas(buffer.length)}`);
        }

        if (!skipParsing) {

            let data = await checkBuffer(buffer, bytesRead);

            if (data.result !== "ok") {
                console.log(`Duplicate key found: ${data.key} in ${file}`);
                process.exit(1);
            }
        }

        // put buffer back in the cache
        buffers.push(buffer);
        totalBytes += bytesRead;
        ++numRead;
        let filename = file.filename ? file.filename : file;
        log(`(${numRead} of ${files.length})   ${path.basename(filename)}    ${calcRate()} MB/s`);
    });

    let delta = Date.now() - start;
    let deltaSeconds = delta / 1000;

    log.now("---------------------------------\nNo conflicting ids found!\n");

    log.now(`Total analyze time = ${addCommas(deltaSeconds)} seconds`);
    log.now(`  number of files:  ${numRead}`);
    log.now(`  numConcurrent:    ${numConcurrent}`);
    log.now(`  numWorkers:       ${numWorkers}`);
    log.now(`  libuvThreads:     ${libuvThreads}`);
    log.now(`  number of MBs:    ${addCommas((totalBytes / (1024 * 1024)).toFixed(0))}`);
    log.now(`  MB per second:    ${addCommas(calcRate())}`);
    log.now(`  readFileTime:     ${readBench.formatSec(3)}`);
    log.now(`  longestWait:      ${longestWait} ms`);
    log.now(`  totalWait:        ${addCommas((Number(totalWait) / (1000 * 1000)).toFixed(0))} ms`);
    log.now("");

    // stop all the workers so the process can execute normally
    for (let worker of workers) {
        await worker.terminate();
    }

    // if we pre-opened files and kept them open, then close them now
    if (preOpenFiles) {
        for (let file of files) {
            await file.close();
        }
    }
}

// for running from the command line
if (require.main === module) {
    analyzeWithWorkers(processArgs(commandLineSpec)).catch(err => {
        console.log(err);
        process.exit(1);
    });
}

// for calling this from another module
module.exports = analyzeWithWorkers;
