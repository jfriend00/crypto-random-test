'use strict';

const fsp = require('fs').promises;

// TODO: Add support for ERR_FS_FILE_TOO_LARGE when buffer can't be made large enough

// Note, if you pass in a buffer, expect that the buffer may not
// be the exact same size as the file is.
// Use the bytesRead return value to know how many bytes in the buffer to pay attention to
// The bufferExtra argument tells fastReadFile to allocate extra space in the buffer if you are
// reusing the same buffer and don't want to be resizing it by small amounts each time
// file argument can be filename or open file handle

// Returns {buffer, bytesRead}
async function fastReadFile(file, buffer = null, bufferExtra = 0) {
    // see if we were passed a filename or a filehandle
    let handle;
    if (typeof file === "string") {
        handle = await fsp.open(file, "r");
    } else {
        handle = file;
    }
    let bytesRead;
    try {
        let stats = await handle.stat();
        if (!buffer || buffer.length < stats.size) {
            let sharedArrayBuffer = new SharedArrayBuffer(stats.size + bufferExtra);
            buffer = Buffer.from(sharedArrayBuffer);
            // put a property on our buffer that lets us get back to the
            // parent sharedArrayBuffer for use in passing to worker threads
            buffer.sharedArrayBuffer = sharedArrayBuffer;
            // buffer = Buffer.allocUnsafe(stats.size + bufferExtra);
        }
        // clear any extra part of the buffer so there's no data leakage
        // from a previous file via the shared buffer
        if (buffer.length > stats.size) {
            buffer.fill(0, stats.size);
        }
        let ret = await handle.read(buffer, 0, stats.size, 0);
        bytesRead = ret.bytesRead;
        if (bytesRead !== stats.size) {
            // no data leaking out
            buffer.fill(0);
            throw new Error("bytesRead not full file size")
        }
    } finally {
        // if we opened the file, then close it
        if (typeof file === "string") {
            // note this does not await the file close
            handle.close().catch(err => {
                console.log(err);
            });
        }
    }
    return {buffer, bytesRead};
}

function parseBufferIntoLines(buffer, bytesRead) {
    let index = 0, targetIndex;
    let lines = [];
    while (index < bytesRead && (targetIndex = buffer.indexOf(10, index)) !== -1) {
        // the buffer may be larger than the actual file data
        // so we have to limit our extraction of data to only what was in the actual file
        let nextIndex = targetIndex + 1;

        // look for CR before LF
        if (buffer[targetIndex - 1] === 13) {
            --targetIndex;
        }
        lines.push(buffer.toString('utf8', index, targetIndex));
        index = nextIndex;
    }
    // check for data at end of file that doesn't end in LF
    if (index < bytesRead) {
        lines.push(buffer.toString('utf8', index, bytesRead));
    }

    return {buffer, lines, bytesRead};
}

// file argument can be filename or file handle
async function fastReadFileLines(file, buf = null, bufferExtra = 0) {
    const {bytesRead, buffer} = await fastReadFile(file, buf, bufferExtra);

    return parseBufferIntoLines(buffer, bytesRead);
}

module.exports = {fastReadFile, fastReadFileLines, parseBufferIntoLines};

// if called directly from command line, run this test function
if (require.main === module) {

//    let buffer = Buffer.alloc(1024 * 1024 * 10, "abc\n", "utf8");
    let buffer = null;

    fastReadFileLines("zzzz", buffer).then(result => {
        let lines = result.lines;
        console.log(lines[0]);
        console.log(lines[1]);
        console.log(lines[2]);
        console.log("...");
        console.log(lines[lines.length - 3]);
        console.log(lines[lines.length - 2]);
        console.log(lines[lines.length - 1]);
    }).catch(err => {
        console.log(err);
    });
}
