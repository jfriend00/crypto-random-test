const crypto = require('crypto');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const base58Encode = require('base-x')('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz').encode;
const makeBucketKey = require('./bucketkey.js');


const {numToTry, numToBatch, workerId} = workerData;

console.log(`worker started ${numToTry}`);

let batch = [];

function sendBatch() {
    if (batch.length) {
        parentPort.postMessage({
            event: "batch",
            data: batch,
            workerId
        });
        batch.length = 0;
    }
}

for (let i = 0; i < numToTry; i++) {
    const orderId = base58Encode(crypto.randomBytes(16));
    const bucketKey = makeBucketKey(orderId);
    batch.push([bucketKey, orderId]);
    if (batch.length % numToBatch === 0) {
        sendBatch();
    }
}

sendBatch();
