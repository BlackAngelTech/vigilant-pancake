function sms(conn, mek) {
    // Add minimal message properties used in pair.js
    return {
        q: mek?.message?.extendedTextMessage?.contextInfo?.quotedMessage || null,
        type: Object.keys(mek.message || {})[0],
        // other fields if needed
    };
}

module.exports = { sms };
