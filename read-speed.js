"use strict";
const processArgs = require("../cmd-line-args");
const { fastReadFile,  fastReadFileLines, parseBufferIntoLines } = require('./fast-read-file.js');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { addCommas } = require('../str-utils');
const { getLogger } = require('../delay-logger');
const { mapConcurrent } = require('../async-utils');

const log = getLogger();

const spec = [
    "-workers=num", 0,
    "-numToRead=num", 0,
    "-numConcurrent=num", 2,
    "-numWorkers=num", 3,
];

// module level configuration variables from command line arguments (with defaults)
let {
    workers,
    numToRead,
    numConcurrent,
    numWorkers
} = processArgs(spec);

class Bench {
    constructor() {
        this.cumT = 0n;
    }
    markBegin() {
        this.startT = process.hrtime.bigint();
    }
    markEnd() {
        this.cumT += process.hrtime.bigint() - this.startT;
    }
    get ms() {
        return Number(this.cumT) / (1000 * 1000);
    }
    get sec() {
        return Number(this.cumT) / (1000 * 1000 * 1000);
    }
    formatMs(decimals = 20) {
        return `${addCommas(this.ms.toFixed(decimals))} ms`;
    }
    formatSec(decimals = 20) {
        return `${addCommas(this.sec.toFixed(decimals))} sec`;
    }
}

// see how fast we can read all the files in the buckets sub-directory

async function run() {
    const dir = "./buckets";
    let files = (await fsp.readdir(dir, {withFileTypes: true}))
      .filter(entry => entry.isFile())
      .map(entry => {
          return path.resolve(path.join(dir, entry.name));
    });
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

    let readBench = new Bench();
    let parseBench = new Bench();

    await mapConcurrent(files, numConcurrent, async (file, index) => {
        // get a buffer from the cache
        let inputBuffer = buffers.length ? buffers.pop() : null;
        let inputBufferLength = inputBuffer ? inputBuffer.length : 0;

        readBench.markBegin();
        let {buffer, bytesRead} = await fastReadFile(file, inputBuffer, 100 * 1024);
        readBench.markEnd();

        parseBench.markBegin();
        let {lines} = parseBufferIntoLines(buffer, bytesRead);
        parseBench.markEnd();

        if (inputBufferLength  !== buffer.length) {
            log.now(`Buffer grown to ${addCommas(buffer.length)}`);
        }
        // put buffer back in the cache
        buffers.push(buffer);
        totalBytes += bytesRead;
        ++numRead;
        log(`(${numRead} of ${files.length})   ${path.basename(file)}    ${calcRate()} MB/s`);
    });

    let delta = Date.now() - start;
    let deltaSeconds = delta / 1000;
    log.now(`\nTotal time = ${addCommas(deltaSeconds)} seconds`);
    log.now(`  number of files:  ${numRead}`);
    log.now(`  numConcurrent:    ${numConcurrent}`);
    log.now(`  libuvThreads:     ${libuvThreads}`);
    log.now(`  number of MBs:    ${addCommas((totalBytes / (1024 * 1024)).toFixed(0))}`);
    log.now(`  MB per second:    ${addCommas(calcRate())}`);
    log.now(`  readFileTime:     ${readBench.formatSec(3)}`);
    log.now(`  parseFileTime:    ${parseBench.formatSec(3)}`);
}

run().catch(err => {
    console.log(err);
});
