
// This is a buffer pool so a bunch of code can share a set of buffers
// It has an option to use shared memory for more efficient transfers
//    without buffer copying between threads
// It is designed for a bunch of fixed size buffers
// options
//    useSharedMemory: true/false        defaults to false
//    zeroBuffer: true/false             defaults to false
class BufferPool {
    constructor(numBuffers, bufSize, opts = {}) {
        this.options = {
            useSharedMemory: false,
            zeroBuffer: false
        }
        Object.assign(this.options, opts);
        this.bufferSize = bufSize;
        this.buffers = [];
        for (let i = 0; i < numBuffers; i++) {
            this.buffers.push(this._makeNewBuffer());
        }
    }
    // intended to be a private method
    _makeNewBuffer() {
        let buffer;
        if (this.options.useSharedMemory) {
            let sharedArrayBuffer = new SharedArrayBuffer(this.bufferSize);
            buffer = Buffer.from(sharedArrayBuffer);
        } else {
            buffer = Buffer.allocUnsafe(this.bufferSize);
        }
        if (this.options.zeroBuffer) {
            buffer.fill(0);
        }
        return buffer;
    }
    // Get a buffer from the pool.  If one does not exist, make a new one
    get() {
        if (this.buffers.length) {
            return this.buffers.pop();
        } else {
            return this._makeNewBuffer();
        }
    }
    // put a  buffer back in the pool
    add(buffer) {
        this.buffers.push(buffer);
    }
}

module.exports = BufferPool;
