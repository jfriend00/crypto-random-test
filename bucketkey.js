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
    return filename.join("").toLowerCase();
}

module.exports = makeBucketKey;
