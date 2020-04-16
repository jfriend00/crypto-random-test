const fsp = require('fs').promises;

async function fastReadFile(filename, buffer = null) {
    let handle = await fsp.open(filename, "r");
    let bytesRead;
    try {
        let stats = await handle.stat();
        if (!buffer || buffer.length < stats.size) {
            buffer = Buffer.allocUnsafe(stats.size);
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
        handle.close().catch(err => {
            console.log(err);
        });
    }
    return {buffer, bytesRead};
}

async function fastReadFileLines(filename, buf = null) {
    const {bytesRead, buffer} = await fastReadFile(filename, buf);

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
    
    return {buffer, lines};
}

module.exports = {fastReadFile, fastReadFileLines};

// if called directly from command line, run this test function
if (require.main === module) {
    
    let buffer = Buffer.alloc(1024 * 1024 * 10, "abc\n", "utf8");

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
