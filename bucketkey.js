const chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const mid = chars.charAt(chars.length / 2);

// option to double our buckets by looking at the third character as above or below midpoint
const doubleBuckets = false;
const reduceBuckets = true;


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


if (reduceBuckets) {
    module.exports = makeBucketKeySmall;
} else {
    module.exports = makeBucketKey;
}
