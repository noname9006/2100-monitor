const ethers = require('ethers');
const { createScheduledMintMessage } = require('./messageConstructor');
require('dotenv').config();

// Enhanced ERC-721 ABI with events for better tracking
const ERC721_ABI = [
    "function totalSupply() view returns (uint256)",
    "function tokenURI(uint256 tokenId) view returns (string)",
    "function ownerOf(uint256 tokenId) view returns (address)",
    "function name() view returns (string)",
    "function symbol() view returns (string)",
    "function balanceOf(address owner) view returns (uint256)",
    "function tokenByIndex(uint256 index) view returns (uint256)",
    "function tokenOfOwnerByIndex(address owner, uint256 index) view returns (uint256)",
    // Events for tracking
    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)"
];

let provider;
let contract;
let lastKnownTokenId = 0;
let lastCheckedBlock = 0;
let lastProcessedTxHash = null;

/**
 * Create provider with version compatibility
 */
function createProvider(rpcUrl) {
    try {
        // Try ethers v6 first
        if (ethers.JsonRpcProvider) {
            console.log('📡 Using ethers v6 JsonRpcProvider');
            return new ethers.JsonRpcProvider(rpcUrl);
        }
        // Fallback to ethers v5
        else if (ethers.providers && ethers.providers.JsonRpcProvider) {
            console.log('📡 Using ethers v5 JsonRpcProvider');
            return new ethers.providers.JsonRpcProvider(rpcUrl);
        }
        // Last resort - try getDefaultProvider
        else {
            console.log('📡 Using ethers getDefaultProvider');
            return ethers.getDefaultProvider(rpcUrl);
        }
    } catch (error) {
        console.error('❌ Error creating provider:', error);
        throw error;
    }
}

/**
 * Scan recent blocks until a mint transaction is found
 */
async function scanForMintTransaction(maxBlocksToScan = 10000) {
    try {
        const currentBlock = await provider.getBlockNumber();
        console.log(`🔍 Starting scan from current block ${currentBlock}...`);
        
        let blocksScanned = 0;
        let batchSize = 1000; // Scan in batches to avoid RPC limits
        let foundMint = null;
        
        while (blocksScanned < maxBlocksToScan && !foundMint) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            
            if (fromBlock >= toBlock) {
                console.log('📊 Reached genesis block, no more blocks to scan');
                break;
            }
            
            console.log(`🔍 Scanning blocks ${fromBlock} to ${toBlock} (batch ${Math.floor(blocksScanned / batchSize) + 1})...`);
            
            try {
                // Get mint events (Transfer from zero address)
                const filter = contract.filters.Transfer(ethers.constants.AddressZero, null);
                const events = await contract.queryFilter(filter, fromBlock, toBlock);
                
                if (events.length > 0) {
                    console.log(`✅ Found ${events.length} mint transaction(s) in blocks ${fromBlock}-${toBlock}`);
                    
                    // Sort events by block number and transaction index to get the latest
                    events.sort((a, b) => {
                        if (a.blockNumber !== b.blockNumber) {
                            return b.blockNumber - a.blockNumber; // Latest block first
                        }
                        return b.transactionIndex - a.transactionIndex; // Latest tx in block first
                    });
                    
                    const latestEvent = events[0];
                    const tokenId = parseInt(latestEvent.args.tokenId.toString());
                    const txHash = latestEvent.transactionHash;
                    const blockNumber = latestEvent.blockNumber;
                    
                    // Get transaction and block details
                    const [tx, block] = await Promise.all([
                        provider.getTransaction(txHash),
                        provider.getBlock(blockNumber)
                    ]);
                    
                    foundMint = {
                        tokenId,
                        txHash,
                        blockNumber,
                        timestamp: block.timestamp,
                        to: latestEvent.args.to,
                        gasUsed: tx.gasLimit ? tx.gasLimit.toString() : 'Unknown',
                        gasPrice: tx.gasPrice ? ethers.utils.formatUnits(tx.gasPrice, 'gwei') : 'Unknown',
                        totalEventsFound: events.length,
                        blocksScannedToFind: blocksScanned + (currentBlock - blockNumber)
                    };
                    
                    console.log(`🎯 Latest mint found: Token ID ${tokenId} in tx ${txHash}`);
                    console.log(`📊 Scanned ${foundMint.blocksScannedToFind} blocks to find mint`);
                    break;
                }
                
                console.log(`📊 No mints found in blocks ${fromBlock}-${toBlock}, continuing scan...`);
                
            } catch (error) {
                console.warn(`⚠️ Error scanning blocks ${fromBlock}-${toBlock}:`, error.message);
                // Continue with smaller batch if RPC error
                if (batchSize > 100) {
                    batchSize = Math.floor(batchSize / 2);
                    console.log(`🔧 Reducing batch size to ${batchSize} blocks`);
                    continue;
                }
            }
            
            blocksScanned += batchSize;
        }
        
        if (!foundMint) {
            console.log(`📊 No mint transactions found after scanning ${blocksScanned} blocks`);
        }
        
        return foundMint;
        
    } catch (error) {
        console.error('❌ Error scanning for mint transaction:', error);
        return null;
    }
}

/**
 * Get the latest minting transaction by scanning until found
 */
async function getLatestMintTransaction() {
    try {
        console.log('🔍 Scanning for latest mint transaction...');
        
        // First try a quick scan of recent blocks
        const quickScan = await scanForMintTransaction(2000);
        if (quickScan) {
            return quickScan;
        }
        
        // If no mint found in recent blocks, do a deeper scan
        console.log('🔍 No recent mints found, performing deeper scan...');
        const deepScan = await scanForMintTransaction(20000);
        
        return deepScan;
        
    } catch (error) {
        console.error('❌ Error getting latest mint transaction:', error);
        return null;
    }
}

/**
 * Get multiple recent mint transactions by scanning until found
 */
async function getRecentMintTransactions(limit = 5) {
    try {
        console.log(`🔍 Scanning for last ${limit} mint transactions...`);
        
        const currentBlock = await provider.getBlockNumber();
        let blocksScanned = 0;
        let batchSize = 2000;
        let allMints = [];
        const maxBlocksToScan = 50000; // Don't scan forever
        
        while (blocksScanned < maxBlocksToScan && allMints.length < limit) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            
            if (fromBlock >= toBlock) break;
            
            console.log(`🔍 Scanning blocks ${fromBlock} to ${toBlock} for mints (found ${allMints.length}/${limit})...`);
            
            try {
                const filter = contract.filters.Transfer(ethers.constants.AddressZero, null);
                const events = await contract.queryFilter(filter, fromBlock, toBlock);
                
                if (events.length > 0) {
                    console.log(`✅ Found ${events.length} mint(s) in blocks ${fromBlock}-${toBlock}`);
                    
                    // Process events and get details
                    for (const event of events) {
                        if (allMints.length >= limit) break;
                        
                        const tokenId = parseInt(event.args.tokenId.toString());
                        const block = await provider.getBlock(event.blockNumber);
                        
                        allMints.push({
                            tokenId,
                            txHash: event.transactionHash,
                            blockNumber: event.blockNumber,
                            timestamp: block.timestamp,
                            to: event.args.to
                        });
                    }
                    
                    // Sort by latest first
                    allMints.sort((a, b) => {
                        if (a.blockNumber !== b.blockNumber) {
                            return b.blockNumber - a.blockNumber;
                        }
                        return b.transactionIndex - a.transactionIndex;
                    });
                    
                    // Keep only the requested number
                    allMints = allMints.slice(0, limit);
                }
                
            } catch (error) {
                console.warn(`⚠️ Error scanning blocks ${fromBlock}-${toBlock}:`, error.message);
                if (batchSize > 500) {
                    batchSize = Math.floor(batchSize / 2);
                    continue;
                }
            }
            
            blocksScanned += batchSize;
        }
        
        console.log(`✅ Found ${allMints.length} mint transactions after scanning ${blocksScanned} blocks`);
        return allMints;
        
    } catch (error) {
        console.error('❌ Error getting recent mint transactions:', error);
        return [];
    }
}

/**
 * Initialize the NFT tracker
 */
async function initializeNFTTracker() {
    try {
        // Validate environment variables
        if (!process.env.CONTRACT_ADDRESS) {
            throw new Error('CONTRACT_ADDRESS environment variable is required');
        }
        if (!process.env.RPC_URL) {
            throw new Error('RPC_URL environment variable is required');
        }

        console.log(`📡 Connecting to RPC: ${process.env.RPC_URL}`);
        
        // Initialize provider with version compatibility
        provider = createProvider(process.env.RPC_URL);
        
        // Test the connection
        console.log('🔍 Testing provider connection...');
        const network = await provider.getNetwork();
        const networkName = network.name || `Chain ID: ${network.chainId}`;
        console.log(`✅ Connected to network: ${networkName}`);

        // Initialize contract
        contract = new ethers.Contract(process.env.CONTRACT_ADDRESS, ERC721_ABI, provider);

        console.log('🔍 Testing contract connection...');
        
        // Get contract info
        let contractName = 'Unknown NFT Collection';
        let contractSymbol = 'NFT';
        
        try {
            contractName = await contract.name();
            contractSymbol = await contract.symbol();
            console.log(`✅ Contract found: ${contractName} (${contractSymbol})`);
        } catch (error) {
            console.warn('⚠️ Could not get contract name/symbol, using defaults.');
        }

        // Get the latest mint to establish baseline
        console.log('🔍 Scanning for latest mint to establish baseline...');
        const latestMint = await getLatestMintTransaction();
        if (latestMint) {
            lastKnownTokenId = latestMint.tokenId;
            lastProcessedTxHash = latestMint.txHash;
            console.log(`📊 Latest minted token ID: ${lastKnownTokenId}`);
            console.log(`📋 Last processed tx: ${lastProcessedTxHash}`);
            console.log(`📦 Found in block: ${latestMint.blockNumber}`);
        } else {
            console.log('📊 No mints found in scanned blocks, starting fresh');
        }

        console.log(`📋 Initialized NFT tracker for ${contractName} (${contractSymbol})`);
        console.log(`📍 Contract address: ${process.env.CONTRACT_ADDRESS}`);
        console.log(`🔧 Tracking method: Deep transaction scanning`);

    } catch (error) {
        console.error('❌ Failed to initialize NFT tracker:', error);
        console.error('💡 Troubleshooting tips:');
        console.error('   - Check your RPC_URL is valid for your EVM network');
        console.error('   - Check your CONTRACT_ADDRESS is correct');
        console.error('   - Make sure your internet connection is working');
        throw error;
    }
}

/**
 * Track new NFT mints by checking latest transactions
 */
async function trackNFT(client) {
    try {
        if (!contract) {
            console.error('❌ Contract not initialized');
            return;
        }

        console.log('🔍 Scanning for new mint transactions...');
        
        // Get the latest mint transaction
        const latestMint = await getLatestMintTransaction();
        
        if (!latestMint) {
            console.log('📊 No mint transactions found in scan');
            // Send scheduled message with current total supply
            try {
                const totalSupply = await contract.totalSupply();
                const totalMints = parseInt(totalSupply.toString());
                await sendDiscordNotification(client, { tokenId: totalMints });
            } catch (error) {
                console.warn('⚠️ Could not get total supply for scheduled message');
                await sendDiscordNotification(client, { tokenId: lastKnownTokenId });
            }
            return;
        }
        
        // Check if this is a new transaction we haven't processed
        if (latestMint.txHash === lastProcessedTxHash) {
            console.log(`📊 No new mints detected. Latest token ID: ${latestMint.tokenId} (tx: ${latestMint.txHash.substring(0, 10)}...)`);
            // Send scheduled message with current total
            await sendDiscordNotification(client, { tokenId: latestMint.tokenId });
            return;
        }
        
        // New mint detected!
        console.log(`🎉 New NFT minted! Token ID: ${latestMint.tokenId}`);
        console.log(`📋 Transaction: ${latestMint.txHash}`);
        console.log(`👤 Minted to: ${latestMint.to}`);
        console.log(`📊 Scanned ${latestMint.blocksScannedToFind} blocks to find this mint`);
        
        // >>>> DO NOT send any Discord message here <<<<

        // Update tracking variables
        lastKnownTokenId = latestMint.tokenId;
        lastProcessedTxHash = latestMint.txHash;

    } catch (error) {
        console.error('❌ Error tracking NFT:', error);
    }
}

/**
 * Send Discord notification about scheduled status
 */
async function sendDiscordNotification(client, mintData) {
    try {
        const { tokenId } = mintData;
        // Get the number of mints in the last 24 hours
        const mints24h = await getMintsCountLast24h();
        // Compose the message with this number
        const message = createScheduledMintMessage(tokenId, mints24h);

        // Add logging for message content
        console.log("📝 Preparing to send Discord message with content:", JSON.stringify(message));

        // Send to specified channel or all available channels
        const channelId = process.env.DISCORD_CHANNEL_ID;
        
        if (channelId) {
            const channel = client.channels.cache.get(channelId);
            if (channel) {
                console.log(`📝 Attempting to send message to channel: ${channel.name} (${channel.id})`);
                await channel.send(message);
                console.log(`📤 Notification sent to channel ${channel.name}`);
            } else {
                console.warn(`⚠️ Could not find channel with ID: ${channelId}`);
            }
        } else {
            // Send to the first available text channel in each guild
            let sentCount = 0;
            for (const guild of client.guilds.cache.values()) {
                const channel = guild.channels.cache.find(ch => 
                    ch.type === 0 && // Text channel
                    ch.permissionsFor(guild.members.me)?.has(['SendMessages', 'EmbedLinks'])
                );
                if (channel) {
                    console.log(`📝 Attempting to send message to guild: ${guild.name}, channel: ${channel.name} (${channel.id})`);
                    await channel.send(message);
                    console.log(`📤 Notification sent to ${guild.name}#${channel.name}`);
                    sentCount++;
                }
            }
            if (sentCount === 0) {
                console.warn('⚠️ No suitable channels found to send notifications');
            }
        }

    } catch (error) {
        console.error('❌ Error sending Discord notification:', error);
    }
}

/**
 * Get current NFT statistics and recent mints
 */
async function getNFTStats() {
    try {
        if (!contract) {
            throw new Error('Contract not initialized');
        }

        let contractName = 'Unknown';
        let contractSymbol = 'Unknown';

        try {
            contractName = await contract.name();
            contractSymbol = await contract.symbol();
        } catch (error) {
            console.warn('⚠️ Could not get contract name/symbol');
        }

        return {
            contractName,
            contractSymbol,
            contractAddress: process.env.CONTRACT_ADDRESS,
            lastKnownTokenId,
            lastProcessedTxHash
        };
    } catch (error) {
        console.error('❌ Error getting NFT stats:', error);
        throw error;
    }
}

/**
 * Manual check for latest mint (useful for testing)
 */
async function checkLatestMint() {
    try {
        console.log('🔍 Manual scan for latest mint...');
        const latestMint = await getLatestMintTransaction();
        
        if (latestMint) {
            console.log(`🎯 Latest mint: Token ID ${latestMint.tokenId}`);
            console.log(`📋 Transaction: ${latestMint.txHash}`);
            console.log(`👤 Minted to: ${latestMint.to}`);
            console.log(`📦 Block: ${latestMint.blockNumber}`);
            console.log(`📅 Time: ${new Date(latestMint.timestamp * 1000).toISOString()}`);
            console.log(`🔍 Blocks scanned: ${latestMint.blocksScannedToFind}`);
        } else {
            console.log('📊 No mints found in scan');
        }
        
        return latestMint;
    } catch (error) {
        console.error('❌ Error checking latest mint:', error);
        return null;
    }
}

/**
 * Get the number of mints in the last 24 hours
 */
async function getMintsCountLast24h() {
    try {
        if (!contract) throw new Error('Contract not initialized');
        const now = Math.floor(Date.now() / 1000);
        const dayAgo = now - 24 * 60 * 60;
        let blocksScanned = 0;
        let batchSize = 2000;
        let firstTokenId = null;
        let lastTokenId = null;
        const currentBlock = await provider.getBlockNumber();
        const maxBlocksToScan = 50000;
        let foundAny = false;
        while (blocksScanned < maxBlocksToScan) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            if (fromBlock >= toBlock) break;
            const filter = contract.filters.Transfer(ethers.constants.AddressZero, null);
            const events = await contract.queryFilter(filter, fromBlock, toBlock);
            for (const event of events) {
                const tokenId = parseInt(event.args.tokenId.toString());
                const block = await provider.getBlock(event.blockNumber);
                const ts = block.timestamp;
                if (ts >= dayAgo && ts <= now) {
                    if (!foundAny) {
                        lastTokenId = tokenId;
                        foundAny = true;
                    }
                    firstTokenId = tokenId;
                }
            }
            blocksScanned += batchSize;
        }
        if (!foundAny) return 0;
        return Math.abs(lastTokenId - firstTokenId) + 1;
    } catch (error) {
        console.error('❌ Error counting mints in last 24h:', error);
        return 0;
    }
}

module.exports = {
    initializeNFTTracker,
    trackNFT,
    getNFTStats,
    checkLatestMint,
    getRecentMintTransactions,
    getMintsCountLast24h
};