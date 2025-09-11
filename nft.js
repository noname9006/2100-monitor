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

let providers = [];
let contracts = [];
let lastKnownTokenIds = [];
let lastProcessedTxHashes = [];

function getContractAddresses() {
    return (process.env.CONTRACT_ADDRESS || '').split(',').map(addr => addr.trim()).filter(Boolean);
}

/**
 * Create provider with version compatibility
 */
function createProvider(rpcUrl) {
    try {
        // Try ethers v6 first
        if (ethers.JsonRpcProvider) {
            console.log(`[${new Date().toISOString()}] INFO: 📡 Using ethers v6 JsonRpcProvider`);
            return new ethers.JsonRpcProvider(rpcUrl);
        }
        // Fallback to ethers v5
        else if (ethers.providers && ethers.providers.JsonRpcProvider) {
            console.log(`[${new Date().toISOString()}] INFO: 📡 Using ethers v5 JsonRpcProvider`);
            return new ethers.providers.JsonRpcProvider(rpcUrl);
        }
        // Last resort - try getDefaultProvider
        else {
            console.log(`[${new Date().toISOString()}] INFO: 📡 Using ethers getDefaultProvider`);
            return ethers.getDefaultProvider(rpcUrl);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error creating provider:`, error);
        throw error;
    }
}

/**
 * Scan recent blocks until a mint transaction is found
 */
async function scanForMintTransaction(maxBlocksToScan = 10000) {
    try {
        const currentBlock = await provider.getBlockNumber();
        console.log(`[${new Date().toISOString()}] INFO: 🔍 Starting scan from current block ${currentBlock}...`);
        
        let blocksScanned = 0;
        let batchSize = 1000; // Scan in batches to avoid RPC limits
        let foundMint = null;
        
        while (blocksScanned < maxBlocksToScan && !foundMint) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            
            if (fromBlock >= toBlock) {
                console.log(`[${new Date().toISOString()}] INFO: 📊 Reached genesis block, no more blocks to scan`);
                break;
            }
            
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Scanning blocks ${fromBlock} to ${toBlock} (batch ${Math.floor(blocksScanned / batchSize) + 1})...`);
            
            try {
                // Get mint events (Transfer from zero address)
                const filter = contract.filters.Transfer(ethers.constants.AddressZero, null);
                const events = await contract.queryFilter(filter, fromBlock, toBlock);
                
                if (events.length > 0) {
                    console.log(`[${new Date().toISOString()}] INFO: ✅ Found ${events.length} mint transaction(s) in blocks ${fromBlock}-${toBlock}`);
                    
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
                    
                    console.log(`[${new Date().toISOString()}] INFO: 🎯 Latest mint found: Token ID ${tokenId} in tx ${txHash}`);
                    console.log(`[${new Date().toISOString()}] INFO: 📊 Scanned ${foundMint.blocksScannedToFind} blocks to find mint`);
                    break;
                }
                
                console.log(`[${new Date().toISOString()}] INFO: 📊 No mints found in blocks ${fromBlock}-${toBlock}, continuing scan...`);
                
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Error scanning blocks ${fromBlock}-${toBlock}:`, error.message);
                // Continue with smaller batch if RPC error
                if (batchSize > 100) {
                    batchSize = Math.floor(batchSize / 2);
                    console.log(`[${new Date().toISOString()}] INFO: 🔧 Reducing batch size to ${batchSize} blocks`);
                    continue;
                }
            }
            
            blocksScanned += batchSize;
        }
        
        if (!foundMint) {
            console.log(`[${new Date().toISOString()}] INFO: 📊 No mint transactions found after scanning ${blocksScanned} blocks`);
        }
        
        return foundMint;
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error scanning for mint transaction:`, error);
        return null;
    }
}

/**
 * Get the latest minting transaction by scanning until found
 */
async function getLatestMintTransaction() {
    try {
        console.log(`[${new Date().toISOString()}] INFO: 🔍 Scanning for latest mint transaction...`);
        
        // First try a quick scan of recent blocks
        const quickScan = await scanForMintTransaction(2000);
        if (quickScan) {
            return quickScan;
        }
        
        // If no mint found in recent blocks, do a deeper scan
        console.log(`[${new Date().toISOString()}] INFO: 🔍 No recent mints found, performing deeper scan...`);
        const deepScan = await scanForMintTransaction(20000);
        
        return deepScan;
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error getting latest mint transaction:`, error);
        return null;
    }
}

/**
 * Get multiple recent mint transactions by scanning until found
 */
async function getRecentMintTransactions(limit = 5) {
    try {
        console.log(`[${new Date().toISOString()}] INFO: 🔍 Scanning for last ${limit} mint transactions...`);
        
        const currentBlock = await provider.getBlockNumber();
        let blocksScanned = 0;
        let batchSize = 2000;
        let allMints = [];
        const maxBlocksToScan = 50000; // Don't scan forever
        
        while (blocksScanned < maxBlocksToScan && allMints.length < limit) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            
            if (fromBlock >= toBlock) break;
            
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Scanning blocks ${fromBlock} to ${toBlock} for mints (found ${allMints.length}/${limit})...`);
            
            try {
                const filter = contract.filters.Transfer(ethers.constants.AddressZero, null);
                const events = await contract.queryFilter(filter, fromBlock, toBlock);
                
                if (events.length > 0) {
                    console.log(`[${new Date().toISOString()}] INFO: ✅ Found ${events.length} mint(s) in blocks ${fromBlock}-${toBlock}`);
                    
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
                console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Error scanning blocks ${fromBlock}-${toBlock}:`, error.message);
                if (batchSize > 500) {
                    batchSize = Math.floor(batchSize / 2);
                    continue;
                }
            }
            
            blocksScanned += batchSize;
        }
        
        console.log(`[${new Date().toISOString()}] INFO: ✅ Found ${allMints.length} mint transactions after scanning ${blocksScanned} blocks`);
        return allMints;
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error getting recent mint transactions:`, error);
        return [];
    }
}

/**
 * Initialize the NFT tracker
 */
async function initializeNFTTracker() {
    try {
        const addresses = getContractAddresses();
        if (!addresses.length) throw new Error('No CONTRACT_ADDRESS specified');
        if (!process.env.RPC_URL) throw new Error('RPC_URL environment variable is required');
        providers = [];
        contracts = [];
        lastKnownTokenIds = [];
        lastProcessedTxHashes = [];
        for (const address of addresses) {
            const provider = createProvider(process.env.RPC_URL);
            providers.push(provider);
            const contract = new ethers.Contract(address, ERC721_ABI, provider);
            contracts.push(contract);
            // Initialize lastKnownTokenIds and lastProcessedTxHashes for each contract
            lastKnownTokenIds.push(0);
            lastProcessedTxHashes.push(null);
        }
        // Optionally, scan for latest mints for each contract
        for (let i = 0; i < contracts.length; i++) {
            const latestMint = await getLatestMintTransactionForContract(contracts[i], providers[i]);
            if (latestMint) {
                lastKnownTokenIds[i] = latestMint.tokenId;
                lastProcessedTxHashes[i] = latestMint.txHash;
            }
        }
        console.log(`[${new Date().toISOString()}] INFO: Initialized NFT tracker for contracts: ${addresses.join(', ')}`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Failed to initialize NFT tracker:`, error);
        throw error;
    }
}

/**
 * Track new NFT mints by checking latest transactions
 */
async function trackNFT(client) {
    try {
        if (!contracts || contracts.length === 0) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Contracts not initialized`);
            return;
        }
        const contract = contracts[0];

        console.log(`[${new Date().toISOString()}] INFO: 🔍 Scanning for new mint transactions...`);
        
        // Get the latest mint transaction
        const latestMint = await getLatestMintTransaction();
        
        if (!latestMint) {
            console.log(`[${new Date().toISOString()}] INFO: 📊 No mint transactions found in scan`);
            // Send scheduled message with current total supply
            try {
                const totalSupply = await contract.totalSupply();
                const totalMints = parseInt(totalSupply.toString());
                await sendDiscordNotification(client, { tokenId: totalMints });
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Could not get total supply for scheduled message`);
                await sendDiscordNotification(client, { tokenId: lastKnownTokenId });
            }
            return;
        }
        
        // Check if this is a new transaction we haven't processed
        if (latestMint.txHash === lastProcessedTxHash) {
            console.log(`[${new Date().toISOString()}] INFO: 📊 No new mints detected. Latest token ID: ${latestMint.tokenId} (tx: ${latestMint.txHash.substring(0, 10)}...)`);
            // Send scheduled message with current total
            await sendDiscordNotification(client, { tokenId: latestMint.tokenId });
            return;
        }
        
        // New mint detected!
        console.log(`[${new Date().toISOString()}] INFO: 🎉 New NFT minted! Token ID: ${latestMint.tokenId}`);
        console.log(`[${new Date().toISOString()}] INFO: 📋 Transaction: ${latestMint.txHash}`);
        console.log(`[${new Date().toISOString()}] INFO: 👤 Minted to: ${latestMint.to}`);
        console.log(`[${new Date().toISOString()}] INFO: 📊 Scanned ${latestMint.blocksScannedToFind} blocks to find this mint`);
        
        // >>>> DO NOT send any Discord message here <<<<

        // Update tracking variables
        lastKnownTokenId = latestMint.tokenId;
        lastProcessedTxHash = latestMint.txHash;

    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error tracking NFT:`, error);
    }
}

/**
 * Send Discord notification about scheduled status
 */
async function sendDiscordNotification(client, mintData) {
    try {
        let { tokenId } = mintData;
        // Получаем значение ADD из переменных окружения и прибавляем к tokenId
        const addValue = parseInt(process.env.ADD || '0', 10);
        if (!isNaN(addValue)) {
            tokenId += addValue;
        }
        // Get the number of mints in the last 24 hours
        const mints24h = await getMintsCountLast24h();
        // Compose the message with this number
        const message = createScheduledMintMessage(tokenId, mints24h);

        // Add logging for message content
        console.log(`[${new Date().toISOString()}] INFO: 📝 Preparing to send Discord message with content:`, JSON.stringify(message));

        // Send to specified channel or all available channels
        const channelId = process.env.DISCORD_CHANNEL_ID;
        
        if (channelId) {
            const channel = client.channels.cache.get(channelId);
            if (channel) {
                console.log(`[${new Date().toISOString()}] INFO: 📝 Attempting to send message to channel: ${channel.name} (${channel.id})`);
                await channel.send(message);
                console.log(`[${new Date().toISOString()}] INFO: 📤 Notification sent to channel ${channel.name}`);
            } else {
                console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Could not find channel with ID: ${channelId}`);
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
                    console.log(`[${new Date().toISOString()}] INFO: 📝 Attempting to send message to guild: ${guild.name}, channel: ${channel.name} (${channel.id})`);
                    await channel.send(message);
                    console.log(`[${new Date().toISOString()}] INFO: 📤 Notification sent to ${guild.name}#${channel.name}`);
                    sentCount++;
                }
            }
            if (sentCount === 0) {
                console.warn(`[${new Date().toISOString()}] WARN: ⚠️ No suitable channels found to send notifications`);
            }
        }

    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error sending Discord notification:`, error);
    }
}

/**
 * Get current NFT statistics and recent mints
 */
async function getNFTStats() {
    try {
        const contract = contracts[0];
        if (!contract) {
            throw new Error('Contract not initialized');
        }

        let contractName = 'Unknown';
        let contractSymbol = 'Unknown';

        try {
            contractName = await contract.name();
            contractSymbol = await contract.symbol();
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Could not get contract name/symbol`);
        }

        return {
            contractName,
            contractSymbol,
            contractAddress: process.env.CONTRACT_ADDRESS,
            lastKnownTokenId,
            lastProcessedTxHash
        };
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error getting NFT stats:`, error);
        throw error;
    }
}

/**
 * Manual check for latest mint (useful for testing)
 */
async function checkLatestMint() {
    try {
        console.log(`[${new Date().toISOString()}] INFO: 🔍 Manual scan for latest mint...`);
        const latestMint = await getLatestMintTransaction();
        
        if (latestMint) {
            console.log(`[${new Date().toISOString()}] INFO: 🎯 Latest mint: Token ID ${latestMint.tokenId}`);
            console.log(`[${new Date().toISOString()}] INFO: 📋 Transaction: ${latestMint.txHash}`);
            console.log(`[${new Date().toISOString()}] INFO: 👤 Minted to: ${latestMint.to}`);
            console.log(`[${new Date().toISOString()}] INFO: 📦 Block: ${latestMint.blockNumber}`);
            console.log(`[${new Date().toISOString()}] INFO: 📅 Time: ${new Date(latestMint.timestamp * 1000).toISOString()}`);
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Blocks scanned: ${latestMint.blocksScannedToFind}`);
        } else {
            console.log(`[${new Date().toISOString()}] INFO: 📊 No mints found in scan`);
        }
        
        return latestMint;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error checking latest mint:`, error);
        return null;
    }
}

/**
 * Get the number of mints in the last 24 hours
 */
async function getMintsCountLast24h() {
    try {
        const contract = contracts[0];
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
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Error counting mints in last 24h:`, error);
        return 0;
    }
}

async function getLatestMintedTokenId(contract, provider) {
    try {
        // Scan backwards in small batches to find the most recent mint event
        const zeroAddress = (ethers.constants && ethers.constants.AddressZero) || ethers.ZeroAddress;
        const filter = contract.filters.Transfer(zeroAddress, null);
        const currentBlock = await provider.getBlockNumber();
        const batchSize = 1000;
        let fromBlock = currentBlock;
        let toBlock = currentBlock;
        while (fromBlock > 0) {
            fromBlock = Math.max(0, toBlock - batchSize + 1);
            const events = await contract.queryFilter(filter, fromBlock, toBlock);
            if (events.length > 0) {
                // Sort events by block number and transaction index descending
                events.sort((a, b) => {
                    if (a.blockNumber !== b.blockNumber) return b.blockNumber - a.blockNumber;
                    return b.transactionIndex - a.transactionIndex;
                });
                // The first event is the most recent
                const latestEvent = events[0];
                const tokenId = parseInt(latestEvent.args.tokenId.toString());
                return tokenId;
            }
            toBlock = fromBlock - 1;
        }
        return 0;
    } catch (error) {
        console.warn(`[${new Date().toISOString()}] WARN: Could not get latest minted tokenId for contract ${contract.address}: ${error.message}`);
        return 0;
    }
}

async function getTotalMints() {
    try {
        if (contracts.length === 0) return 0;
        let sum = 0;
        for (let i = 0; i < contracts.length; i++) {
            const tokenId = await getLatestMintedTokenId(contracts[i], providers[i]);
            sum += tokenId;
        }
        return sum;
    } catch (error) {
        console.warn(`[${new Date().toISOString()}] WARN: Could not sum total mints across contracts: ${error.message}`);
        return 0;
    }
}

async function getLastTokenIdForContract(contract, provider) {
    try {
        // Try to get the latest mint transaction for this contract
        const latestMint = await getLatestMintTransactionForContract(contract, provider);
        if (latestMint) {
            return latestMint.tokenId;
        }
        // Fallback to totalSupply
        const totalSupply = await contract.totalSupply();
        return parseInt(totalSupply.toString());
    } catch (error) {
        console.warn(`[${new Date().toISOString()}] WARN: Could not get last tokenId for contract ${contract.address}: ${error.message}`);
        return 0;
    }
}

async function getLatestMintTransactionForContract(contract, provider) {
    // Copy of getLatestMintTransaction, but for a specific contract/provider
    try {
        // ... similar to getLatestMintTransaction, but use contract and provider passed in ...
        // For brevity, you can call scanForMintTransactionForContract(contract, provider, 2000) and fallback to 20000
        const quickScan = await scanForMintTransactionForContract(contract, provider, 2000);
        if (quickScan) return quickScan;
        return await scanForMintTransactionForContract(contract, provider, 20000);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Error getting latest mint transaction for contract ${contract.address}:`, error);
        return null;
    }
}

async function scanForMintTransactionForContract(contract, provider, maxBlocksToScan = 10000) {
    try {
        const currentBlock = await provider.getBlockNumber();
        let blocksScanned = 0;
        let batchSize = 1000;
        let foundMint = null;
        while (blocksScanned < maxBlocksToScan && !foundMint) {
            const fromBlock = Math.max(0, currentBlock - blocksScanned - batchSize);
            const toBlock = currentBlock - blocksScanned;
            if (fromBlock >= toBlock) break;
            const zeroAddress = (ethers.constants && ethers.constants.AddressZero) || ethers.ZeroAddress;
            const filter = contract.filters.Transfer(zeroAddress, null);
            const events = await contract.queryFilter(filter, fromBlock, toBlock);
            if (events.length > 0) {
                events.sort((a, b) => {
                    if (a.blockNumber !== b.blockNumber) return b.blockNumber - a.blockNumber;
                    return b.transactionIndex - a.transactionIndex;
                });
                const latestEvent = events[0];
                const tokenId = parseInt(latestEvent.args.tokenId.toString());
                const txHash = latestEvent.transactionHash;
                const blockNumber = latestEvent.blockNumber;
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
                break;
            }
            blocksScanned += batchSize;
        }
        return foundMint;
    } catch (error) {
        console.warn(`[${new Date().toISOString()}] WARN: Error scanning for mint transaction for contract ${contract.address}:`, error.message);
        return null;
    }
}

module.exports = {
    initializeNFTTracker,
    trackNFT,
    getNFTStats,
    checkLatestMint,
    getRecentMintTransactions,
    getMintsCountLast24h,
    getTotalMints
};