/**
 * Discord Message Constructor Module
 * Handles creating Discord messages for NFT mint notifications
 */

/**
 * Create a scheduled message with total mints count, date/time, and user login
 * @param {number} totalMints - The total number of mints (based on token ID)
 * @returns {Object} Discord message object with embed
 */
function createScheduledMintMessage(totalMints) {
    // Get current date and time in UTC
    const now = new Date();
    const utcDateTime = now.toISOString().replace('T', ' ').substring(0, 19);
    
    return {
        embeds: [{
            color: 0xFFD700, // Gold color
            description: `Total mints: ${totalMints}\nCurrent Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): ${utcDateTime}\nCurrent User's Login: noname9006`
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

/**
 * Create a mint notification message for Discord
 * @param {Object} mintData - Mint transaction data
 * @returns {Object} Discord message object with embed
 */
function createMintNotification(mintData) {
    const { tokenId, txHash, to, blockNumber, timestamp } = mintData;
    const mintTime = new Date(timestamp * 1000).toISOString();
    
    return {
        embeds: [{
            color: 0x9932CC, // Purple color for mint notifications
            title: "🎉 New NFT Mint Detected!",
            description: `**Token ID:** ${tokenId}\n**Transaction:** \`${txHash}\`\n**Minted to:** \`${to}\`\n**Block:** ${blockNumber}\n**Time:** ${mintTime}`,
            timestamp: new Date(timestamp * 1000).toISOString()
        }]
    };
}

module.exports = {
    createScheduledMintMessage,
    createMintEventLog,
    createCustomNotification,
    createErrorNotification,
    createStatusNotification,
    createMintNotification
};