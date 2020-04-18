const crypto = require('crypto');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const base58Encode = require('base-x')('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz').encode;
const makeBucketKey = require('./bucketkey.js');
const addCommas = require('./addcommas.js');


const {numToTry, numToBatch, workerId} = workerData;

console.log(`worker #${workerId} started - assigned ${addCommas(numToTry)} ids to make`);

// state
const batch = [];
let numRemaining = numToTry;

function sendBatch() {
    if (batch.length) {
        parentPort.postMessage({
            event: "batch",
            data: batch,
            workerId
        });
        batch.length = 0;
    }
    if (numRemaining === 0) {
        // all work done, allow the worker to die and thusly parent to exit
        parentPort.removeListener('message', handleMessage);
    }
}

function makeBatch() {
    while (numRemaining > 0 && batch.length < numToBatch) {
        const orderId = base58Encode(crypto.randomBytes(16));
        const bucketKey = makeBucketKey(orderId);
        batch.push([bucketKey, orderId]);
        --numRemaining;
    }
}

function handleMessage(msg) {
    if (msg.event === "batch") {
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
