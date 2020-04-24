const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { parseBufferIntoLines } = require('./fast-read-file.js');

const {workerId, doBinary, keyLen} = workerData;

console.log(`worker #${workerId} started`);

// very simple encoding just to convert our 16 byte binary to a string
// that can be used in a Set object
function encodeDecimal(buffer, start, len, arrayToUse) {
    if (len === undefined) {
        len = buffer.length;
    }
    let output = arrayToUse ? arrayToUse : new Array(len);
    for (let i = 0; i < len; i++) {
        output[i] = buffer[start + i];
    }
    return output.join(",");
}


function handleMessage(msg) {
    if (msg.event === "checkBuffer") {
        let buffer = Buffer.from(msg.data.sharedArrayBuffer);
        let dataLength = msg.data.dataLength;
        if (doBinary) {
            // processing binary files here
            if (dataLength % keyLen !== 0) {
                throw new Error("Sent uneven length buffer, not a multiple of keyLen");
            }
            let num = dataLength / keyLen;
            let set = new Set();
            let array = new Array(keyLen);
            let index = 0;
            for (let i = 0; i < num; i++) {
                let str = encodeDecimal(buffer, index, keyLen, array);
                if (set.has(str)) {
                    // send result back that we found a dup
                    console.log(`Duplicate key found ${str}`);
                    parentPort.postMessage({
                        event: "result",
                        data: {
                            result: "duplicate",
                            key: str
                        }
                    });
                    break;
                } else {
                    set.add(str);
                }
                index += keyLen;
            }
            set = null;
            parentPort.postMessage({
                event: "result",
                data: { result: "ok" }
            });
        } else {

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
}

// Worker is sent a message when there is work to do
// Parent will not send it additional work until a result is sent back
parentPort.on('message', handleMessage);
