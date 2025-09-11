const { getTotalMints } = require('./nft.js');
const { getTotalSpins } = require('./spin.js');

async function updateCounters(client) {
    if (!client) {
        console.error(`[${new Date().toISOString()}] ERROR: Discord client is required for updating counters.`);
        return;
    }

    // Counter 1: Total Mints
    try {
        const channelId1 = process.env.COUNTER1_CHANNEL_ID;
        const nameTemplate1 = process.env.COUNTER1_NAME;
        console.log(`[${new Date().toISOString()}] DEBUG: COUNTER1_CHANNEL_ID=${channelId1}, COUNTER1_NAME=${nameTemplate1}`);
        if (channelId1 && nameTemplate1) {
            const totalMints = await getTotalMints();
            const addValue = parseInt(process.env.ADD || '0', 10);
            const displayMints = totalMints + (isNaN(addValue) ? 0 : addValue);
            console.log(`[${new Date().toISOString()}] DEBUG: totalMints=${totalMints}, addValue=${addValue}, displayMints=${displayMints}`);
            let channel;
            try {
                channel = await client.channels.fetch(channelId1);
                console.log(`[${new Date().toISOString()}] DEBUG: Fetched channel for COUNTER1_CHANNEL_ID: ${!!channel}`);
            } catch (fetchErr) {
                console.error(`[${new Date().toISOString()}] ERROR: Could not fetch channel for COUNTER1_CHANNEL_ID:`, fetchErr);
            }
            if (channel) {
                const newName = nameTemplate1.replace('{count}', displayMints);
                try {
                    await channel.setName(newName);
                    console.log(`[${new Date().toISOString()}] INFO: Updated counter 1 channel name to: ${newName}`);
                } catch (setNameErr) {
                    console.error(`[${new Date().toISOString()}] ERROR: Failed to set channel name for COUNTER1_CHANNEL_ID:`, setNameErr);
                }
            } else {
                console.warn(`[${new Date().toISOString()}] WARN: Channel not found or not accessible for COUNTER1_CHANNEL_ID`);
            }
        } else {
            console.warn(`[${new Date().toISOString()}] WARN: COUNTER1_CHANNEL_ID or COUNTER1_NAME not set in .env`);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Failed to update counter 1:`, error.message);
    }

    // Counter 2: Total Spins
    try {
        const channelId2 = process.env.COUNTER2_CHANNEL_ID;
        const nameTemplate2 = process.env.COUNTER2_NAME;
        console.log(`[${new Date().toISOString()}] DEBUG: COUNTER2_CHANNEL_ID=${channelId2}, COUNTER2_NAME=${nameTemplate2}`);
        if (channelId2 && nameTemplate2) {
            const totalSpins = await getTotalSpins();
            console.log(`[${new Date().toISOString()}] DEBUG: totalSpins=${totalSpins}`);
            let channel;
            try {
                channel = await client.channels.fetch(channelId2);
            } catch (fetchErr) {
                console.error(`[${new Date().toISOString()}] ERROR: Could not fetch channel for COUNTER2_CHANNEL_ID:`, fetchErr);
            }
            if (channel) {
                const newName = nameTemplate2.replace('{count}', totalSpins);
                try {
                    await channel.setName(newName);
                    console.log(`[${new Date().toISOString()}] INFO: Updated counter 2 channel name to: ${newName}`);
                } catch (setNameErr) {
                    console.error(`[${new Date().toISOString()}] ERROR: Failed to set channel name for COUNTER2_CHANNEL_ID:`, setNameErr);
                }
            } else {
                console.warn(`[${new Date().toISOString()}] WARN: Channel not found or not accessible for COUNTER2_CHANNEL_ID`);
            }
        } else {
            console.warn(`[${new Date().toISOString()}] WARN: COUNTER2_CHANNEL_ID or COUNTER2_NAME not set in .env`);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Failed to update counter 2:`, error.message);
    }
}

module.exports = {
    updateCounters
}; 