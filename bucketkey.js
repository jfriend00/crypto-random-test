const chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const mid = chars.charAt(chars.length / 2);

// option to double our buckets by looking at the third character as above or below midpoint
const doubleBuckets = false;
const reduceBuckets = true;

const zeroPad = "00000000000000";
const targetLen = 4;

// Use the first two bytes of a buffer to generate a bucket string value
// that can be used for a unique filename string
function makeBucketKeyBinary(buffer) {
    // we want around 1600 buckets
    // first byte gets us 256
    // 1600/256 = 6.25
    // so we need some extra bits to get up to 1600 buckets
    // 9 bits =  512
    // 10 bits = 1024
    // 11 bits = 2048
    // So our choice is somewhat between 10 and 11 bits
    // Currently implemented for 11 bits
    let byte1 = buffer[0];                           // first 8 bits
    let byte2 = buffer[1] & 0b00000111;              // next 3 bits, shifted into low position
    let bucketNum = byte1 + (256 * byte2);
    let bucketStr = "" + bucketNum;
    // normalize the length with leading zero padding
    return zeroPad.slice(0, targetLen - bucketStr.length) + bucketStr;
}


// make a unique filename using first few letters of
// the string.  Strings are case sensitive, bucket filenames
// cannot be so it has to be case neutralized while retaining
// uniqueness
function makeBucketKey(str) {
    // with base58 algorithm, the first character does not have as much randomness
    // so we pick the 2nd and 3rd characters for the bucket keys and this makes
    // much more even buckets
    let piece = str.substr(1,2);
    let filename = [];
    // double up each character, but
    for (let ch of piece) {
        filename.push(ch);
        if (ch >= 'a' && ch <= 'z') {
            filename.push("_");
        } else {
            filename.push(ch);
        }
    }
    if (doubleBuckets) {
        if (str.charAt(3) > mid) {
            filename.push("1");
        } else {
            filename.push("2");
        }
    }
    return filename.join("").toLowerCase();
}

// reduce to half as many buckets
function makeBucketKeySmall(str) {
    // with base58 algorithm, the first character does not have as much randomness
    // so we pick the 2nd and 3rd characters for the bucket keys and this makes
    // much more even buckets
    let piece = str.substr(1,2);
    let filename = [];
    // double up each character, but
    let ch = piece.charAt(0);
    filename.push(ch);
    if (ch >= 'a' && ch <= 'z') {
        filename.push("_");
    } else {
        filename.push(ch);
    }

    // for char codes at an odd index, round them down by one (to combine neighboring odd/even)
    // and reduce the buckets by half
    ch = piece.charAt(1);
    let index = chars.indexOf(ch);
    // if character is odd, round it down one to combine to buckets to one
    if (index % 2 !== 0) {
        ch = chars.charAt(index - 1);
    }
    filename.push(ch);
    if (ch >= 'a' && ch <= 'z') {
        filename.push("_");
    } else {
        filename.push(ch);
    }
    return filename.join("").toLowerCase();
}

module.exports = function makeBucketKeyGeneric(data) {
    if (typeof data === "string") {
        if (reduceBuckets) {
            return makeBucketKeySmall(data);
        } else {
            return makeBucketKey(data);
        }
    } else {
        return makeBucketKeyBinary(data);
    }
}
