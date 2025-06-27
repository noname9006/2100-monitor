/**
 * Discord Message Constructor Module
 * Handles creating Discord messages for NFT mint notifications
 */

/**
 * Create a scheduled message with total mints count, date/time, and user login
 * @param {number} totalMints - The total number of mints (based on token ID)
 * @param {number} mints24h - The number of mints in the last 24 hours
 * @returns {Object} Discord message object with embed
 */


function createScheduledMintMessage(totalMints, mints24h) {
    // Get current date and time in UTC
    const now = new Date();
    const utcDateTime = now.toISOString().replace('T', ' ').substring(0, 19);
    
    return {
        embeds: [{
            title: "Citadel Invitations",
            color: 0xFFD700, // Gold color
            thumbnail: {
                url: "https://media.discordapp.net/attachments/1317881540176248904/1388171834075123772/ezgif-6817638b410628_copy.png"
            },
            description: `Last 24 hours: ${mints24h} mints\nTotal mints: ${totalMints}`
        }]
    };
}

/**
 * Create a mint event log message (for console logging)
 * @param {Object} mintData - Mint transaction data
 * @returns {string} Formatted log message
 */
function createMintEventLog(mintData) {
    const { tokenId, txHash, to, blockNumber, timestamp } = mintData;
    const mintTime = new Date(timestamp * 1000).toISOString();
    
    return `🎉 MINT EVENT DETECTED:\n` +
           `   Token ID: ${tokenId}\n` +
           `   Transaction: ${txHash}\n` +
           `   Minted to: ${to}\n` +
           `   Block: ${blockNumber}\n` +
           `   Time: ${mintTime}`;
}

/**
 * Create a notification message with custom content
 * @param {string} content - Custom content for the embed
 * @returns {Object} Discord message object with embed
 */
function createCustomNotification(content) {
    return {
        embeds: [{
            color: 0xFFD700, // Gold color
            description: content
        }]
    };
}

/**
 * Create an error notification message
 * @param {string} errorMessage - Error message to display
 * @returns {Object} Discord message object with embed
 */
function createErrorNotification(errorMessage) {
    return {
        embeds: [{
            color: 0xFF0000, // Red color
            description: `❌ Error: ${errorMessage}`
        }]
    };
}

/**
 * Create a status notification message
 * @param {string} statusMessage - Status message to display
 * @returns {Object} Discord message object with embed
 */
function createStatusNotification(statusMessage) {
    return {
        embeds: [{
            color: 0x00FF00, // Green color
            description: `ℹ️ ${statusMessage}`
        }]
    };
}

module.exports = {
    createScheduledMintMessage,
    createMintEventLog,
    createCustomNotification,
    createErrorNotification,
    createStatusNotification
};