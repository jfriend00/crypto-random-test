const { Worker } = require('worker_threads');
const addCommas = require('./addcommas.js');

let numToTry = 10_000_000;
let numToBatch = 1_000;           // how many for worker to accumulate before sending array to parent
let numWorkers = 6;

async function generateRandomsWorkers() {
    return new Promise((resolve, reject) => {
        let workers = new Set();
        let keysReceived = 0;
        let randomsRemaining = numToTry;
        let randomsPerWorker = Math.ceil(numToTry / numWorkers);
        for (let i = 0; i < numWorkers; i++) {
            let num = Math.min(randomsRemaining, randomsPerWorker);
            let worker = new Worker("./worker.js", {
                workerData: {numToTry: num, numToBatch: numToBatch, workerId: i}
            });
            workers.add(worker);
            randomsRemaining -= num;

            worker.on('message', msg => {
                if (msg.event === "batch") {
                    let data = msg.data;
                    keysReceived += data.length;
                    //console.log(`Incoming ${data.length} keys from WorkerId ${msg.workerId}, total keysReceived = ${keysReceived}`);
                    //console.log(data);
                }
            });

            worker.on('exit', code => {
                console.log(`worker ${i} exited`);
                workers.delete(worker);
                if (workers.size === 0) {
                    resolve();
                }
            });

            worker.on('error', err => {
                console.log(`worker ${i} had error`, err);
                reject(err);
            });
        }
        console.log(`randomsRemaining = ${randomsRemaining}`);
    });
}

let t1 = Date.now();
generateRandomsWorkers().then(() => {
    let t2 = Date.now();
    console.log(`Total Time with ${numWorkers} worker is ${addCommas((t2 - t1)/1000)}sec`);
}).catch(err => {
    console.log(err);
});
