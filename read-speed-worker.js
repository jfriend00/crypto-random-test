const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { parseBufferIntoLines } = require('./fast-read-file.js');

const {workerId} = workerData;

console.log(`worker #${workerId} started`);

function handleMessage(msg) {
    if (msg.event === "checkBuffer") {
        let buffer = Buffer.from(msg.data.sharedArrayBuffer);
        let dataLength = msg.data.dataLength;
        let { lines } = parseBufferIntoLines(buffer, dataLength);
        let set = new Set();
        for (let line of lines) {
            if (set.has(line)) {
                // send result back that we found a dup
                console.log(`Duplicate key found ${line}`);
                parentPort.postMessage({
                    event: "result",
                    data: {
                        result: "duplicate",
                        key: line
                    }
                });
                break;
            } else {
                set.add(line);
            }
        }
        set = null;
        parentPort.postMessage({
            event: "result",
            data: { result: "ok" }
        });
    }
}

// Worker is sent a message when there is work to do
// Parent will not send it additional work until a result is sent back
parentPort.on('message', handleMessage);
