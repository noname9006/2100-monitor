const { Client, GatewayIntentBits } = require('discord.js');
const cron = require('node-cron');
const { trackNFT, initializeNFTTracker } = require('./nft.js');
const { createSatoshiSpinEmbed } = require('./spin.js');
const { updateCounters } = require('./counters.js');
const { StbtcCounters } = require('./stbtc_counters.js');

require('dotenv').config();

// Create Discord client
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent
    ]
});

// Initialize STBTC counters
let stbtcCounters;

// Bot ready event
client.once('ready', async () => {
    console.log(`[${new Date().toISOString()}] INFO: ✅ ${client.user.tag} is online!`);
    
    // Initialize NFT tracker
    await initializeNFTTracker();
    
    // Initialize STBTC counters
    stbtcCounters = new StbtcCounters(client);
    stbtcCounters.start();
    console.log(`[${new Date().toISOString()}] INFO: 🚀 STBTC counters initialized and started`);
    
    // Set up cron job for messages
    const msgCronSchedule = process.env.CRON_SCHEDULE_MSG || '*/5 * * * *';
    console.log(`[${new Date().toISOString()}] INFO: 🕒 Setting up message cron job with schedule: ${msgCronSchedule}`);
    cron.schedule(msgCronSchedule, async () => {
        // Send scheduled Discord message for NFT Invitations
        try {
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Running scheduled NFT check...`);
            await trackNFT(client);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error during scheduled NFT check:`, error);
        }

        // Send scheduled Discord message for Satoshi Spins
        try {
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Running scheduled Spin check...`);
            const spinEmbed = await createSatoshiSpinEmbed();
            const channelId = process.env.DISCORD_CHANNEL_ID;
            if (channelId) {
                const channel = client.channels.cache.get(channelId);
                if (channel) {
                    await channel.send(spinEmbed);
                    console.log(`[${new Date().toISOString()}] INFO: 📤 Satoshi Spin message sent to channel ${channel.name}`);
                } else {
                    console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Could not find channel with ID: ${channelId}`);
                }
            } else {
                // Fallback to first available channel if no ID is set
                let sent = false;
                for (const guild of client.guilds.cache.values()) {
                    const channel = guild.channels.cache.find(ch => 
                        ch.type === 0 && // Text channel
                        ch.permissionsFor(guild.members.me)?.has(['SendMessages', 'EmbedLinks'])
                    );
                    if (channel) {
                        await channel.send(spinEmbed);
                        console.log(`[${new Date().toISOString()}] INFO: 📤 Satoshi Spin message sent to channel ${channel.name} in guild ${guild.name}`);
                        sent = true;
                        break; // Send to only one channel
                    }
                }
                if (!sent) {
                    console.warn(`[${new Date().toISOString()}] WARN: ⚠️ No suitable channels found to send spin notifications`);
                }
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error during scheduled Spin check:`, error);
        }
    });

    // Set up cron job for counters (existing counters)
    const counterCronSchedule = process.env.CRON_SCHEDULE_COUNTER;
    if (counterCronSchedule) {
        console.log(`[${new Date().toISOString()}] INFO: 🕒 Setting up counter cron job with schedule: ${counterCronSchedule}`);
        cron.schedule(counterCronSchedule, async () => {
            try {
                console.log(`[${new Date().toISOString()}] INFO: 🔄 Running scheduled counter update...`);
                await updateCounters(client);
            } catch (error) {
                console.error(`[${new Date().toISOString()}] ERROR: ❌ Error during scheduled counter update:`, error);
            }
        });
    } else {
        console.log(`[${new Date().toISOString()}] INFO: ℹ️ Counter cron job not scheduled, CRON_SCHEDULE_COUNTER not set.`);
    }
});

// Error handling
client.on('error', (error) => {
    console.error(`[${new Date().toISOString()}] ERROR: ❌ Discord client error:`, error);
});

process.on('unhandledRejection', (error) => {
    console.error(`[${new Date().toISOString()}] ERROR: ❌ Unhandled promise rejection:`, error);
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log(`[${new Date().toISOString()}] INFO: 🔄 Gracefully shutting down...`);
    if (stbtcCounters) {
        stbtcCounters.stop();
    }
    client.destroy();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log(`[${new Date().toISOString()}] INFO: 🔄 Gracefully shutting down...`);
    if (stbtcCounters) {
        stbtcCounters.stop();
    }
    client.destroy();
    process.exit(0);
});

// Login to Discord
client.login(process.env.DISCORD_TOKEN).catch((error) => {
    console.error(`[${new Date().toISOString()}] ERROR: ❌ Failed to login to Discord:`, error);
    process.exit(1);
});