const { Client, GatewayIntentBits } = require('discord.js');
const cron = require('node-cron');
const { trackNFT, initializeNFTTracker } = require('./nft.js');
require('dotenv').config();

// Create Discord client
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent
    ]
});

// Bot ready event
client.once('ready', async () => {
    console.log(`✅ ${client.user.tag} is online!`);
    
    // Initialize NFT tracker
    await initializeNFTTracker();
    
    // Set up cron job for NFT tracking
    const cronSchedule = process.env.CRON_SCHEDULE || '*/5 * * * *'; // Default: every 5 minutes
    console.log(`🕒 Setting up cron job with schedule: ${cronSchedule}`);
    
    cron.schedule(cronSchedule, async () => {
        console.log('🔍 Running scheduled NFT check...');
        try {
            // Send scheduled Discord message
            await trackNFT(client);
        } catch (error) {
            console.error('❌ Error during scheduled NFT check:', error);
        }
    });
});

// Error handling
client.on('error', (error) => {
    console.error('❌ Discord client error:', error);
});

process.on('unhandledRejection', (error) => {
    console.error('❌ Unhandled promise rejection:', error);
});

// Login to Discord
client.login(process.env.DISCORD_TOKEN).catch((error) => {
    console.error('❌ Failed to login to Discord:', error);
    process.exit(1);
});