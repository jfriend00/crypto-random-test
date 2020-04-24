"use strict";

const crypto = require('crypto');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const base58Encode = require('base-x')('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz').encode;
const {makeBucketKey, makeBucketKeyBinary} = require('./bucketkey.js');
const { addCommas } = require('../str-utils');
const { Bench } = require('../measure');
const BufferPool = require('./buffer-pool.js');

const {numToTry, numToBatch, workerId, doBinary, keyLen} = workerData;

console.log(`worker #${workerId} started - assigned ${addCommas(numToTry)} ids to make`);

const measureTotal = new Bench().markBegin();
const measureBusy = new Bench();

// state
const batch = [];
let binaryBatch = null;
let numRemaining = numToTry;
// create two buffers, one for sending and one for working on the next while sending
const bufferPool = new BufferPool(2, numToBatch * keyLen, {useSharedMemory: true});

function sendBatch() {
    measureBusy.markBegin();
    if (batch.length || binaryBatch) {
        let msg = {event: "batch", workerId, postMessageTime: process.hrtime.bigint()};
        if (doBinary) {
            msg.data = binaryBatch;
        } else {
            msg.data = batch;
        }
        parentPort.postMessage(msg);
        // clear these since they've been sent
        batch.length = 0;
        binaryBatch = null;
    }
    measureBusy.markEnd();
    if (numRemaining === 0) {
        // all work done, allow the worker to die and thusly parent to exit
        measureTotal.markEnd();
        parentPort.postMessage({
            event: "usage",
            data: {
                measureTotal: measureTotal.ns,
                measureBusy: measureBusy.ns,
                busyPercentage: (measureBusy.nsN / measureTotal.nsN) * 100,
            },
            postMessageTime: process.hrtime.bigint(),
            workerId: workerId
        });
        parentPort.removeListener('message', handleMessage);
    }
}

const makeBatch = doBinary ? makeBatchBinary : makeBatchText;

function makeBatchBinary() {
    measureBusy.markBegin();
    let buffer = bufferPool.get();
    if (!buffer) {
        throw new Error(`Buffer pool empty in workerId: ${workerId}`);
    }
    let index = 0;
    let batchCntr = 0;
    while (numRemaining > 0 && batchCntr < numToBatch) {
        const idBuf = crypto.randomBytes(keyLen);
        // copy the randomBytes into our larger buffer
        idBuf.copy(buffer, index);
        index += keyLen;
        ++batchCntr;
        --numRemaining;
    }
    measureBusy.markEnd();
    // save this buffer for later use when sending
    if (binaryBatch) {
        throw new Error("Were about to overwrite binaryBatch");
    }
    binaryBatch = {sharedArrayBuffer: buffer.buffer, cnt: batchCntr};
}

function makeBatchText() {
    measureBusy.markBegin();
    while (numRemaining > 0 && batch.length < numToBatch) {
        const orderId = base58Encode(crypto.randomBytes(keyLen));
        const bucketKey = makeBucketKey(orderId);
        batch.push([bucketKey, orderId]);
        --numRemaining;
    }
    measureBusy.markEnd();
}

function handleMessage(msg) {
    if (msg.event === "batch") {
        // if this came with a buffer, then put it in our pool
        if (msg.data && msg.data.buffer) {
            bufferPool.add(Buffer.from(msg.data.buffer));
        }
        sendBatch();        // send batch we have
        makeBatch();        // get one ready for next time they ask
    }
}

// Workers send a batch only when they receive a "batch" message
// parent must send a batch message to start the worker process and then after each batch is received
// In this way, the parent can implement flow control at the batch level
parentPort.on('message', handleMessage);

// make a batch ahead of time so it's ready as soon as they ask for it
makeBatch();
