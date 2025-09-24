const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const cron = require('node-cron');

require('dotenv').config();

class TransactionTracker {
    constructor() {
        this.address = process.env.ADDRESS1;
        this.rpcUrl = process.env.RPC_URL;
this.cronSchedule = process.env.CRON_SCHEDULE_BLOCKCHAIN;
        this.startBlock = parseInt(process.env.START_BLOCK || '1'); // NEW: Configurable start block
        this.csvFilename = null;
        this.progressFilename = null;
        this.isRunning = false;
        this.lastProcessedBlock = 0;
        this.processedBlocks = new Set();
        this.initialScanCompleted = false;
        
        // Performance settings
        this.maxConcurrentRequests = parseInt(process.env.MAX_CONCURRENT_REQUESTS || '20');
        this.batchSize = parseInt(process.env.BATCH_SIZE || '1000');
        this.requestDelay = parseInt(process.env.REQUEST_DELAY || '10');
        
        // Validate required environment variables
        if (!this.address) {
            throw new Error('ADDRESS1 environment variable is required');
        }
        if (!this.rpcUrl) {
            throw new Error('RPC_URL environment variable is required');
        }
        
        // Validate START_BLOCK
        if (this.startBlock < 1) {
            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Invalid START_BLOCK (${process.env.START_BLOCK}), using block 1`);
            this.startBlock = 1;
        }
        
        // Set filenames based on contract address
        this.csvFilename = `${this.address.toLowerCase()}.csv`;
        this.progressFilename = `${this.address.toLowerCase()}_progress.json`;
        
        console.log(`[${new Date().toISOString()}] INFO: 🚀 TransactionTracker initialized for address: ${this.address}`);
        console.log(`[${new Date().toISOString()}] INFO: 🎯 Start block: ${this.startBlock} (will not scan earlier blocks)`);
        console.log(`[${new Date().toISOString()}] INFO: 📊 CSV file: ${this.csvFilename}`);
        console.log(`[${new Date().toISOString()}] INFO: 📊 Progress file: ${this.progressFilename}`);
        console.log(`[${new Date().toISOString()}] INFO: ⚡ Performance: ${this.maxConcurrentRequests} concurrent requests, ${this.batchSize} batch size`);
    }

    /**
     * Initialize the transaction tracker
     */
    async initialize() {
        try {
            // Check if CSV file exists, create if not
            await this.ensureCsvFile();
            
            // Load progress from both CSV and progress file
            await this.loadProgress();
            
            // Perform initial scan on launch
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Starting initial transaction scan on launch...`);
            await this.performInitialScan();
            
            console.log(`[${new Date().toISOString()}] INFO: ✅ TransactionTracker initialized successfully`);
            console.log(`[${new Date().toISOString()}] INFO: 📊 Last processed block: ${this.lastProcessedBlock}`);
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Failed to initialize TransactionTracker:`, error);
            throw error;
        }
    }

    /**
     * Load progress from both CSV and progress file - RESPECTS START_BLOCK
     */
    async loadProgress() {
        try {
            // Try to load from progress file first (more reliable)
            await this.loadFromProgressFile();
            
            // If no progress file, fall back to CSV analysis
            if (this.lastProcessedBlock === 0) {
                await this.loadFromCSV();
            }
            
            // Ensure we never go below START_BLOCK
            if (this.lastProcessedBlock < this.startBlock) {
                console.log(`[${new Date().toISOString()}] INFO: 📊 Adjusting last processed block from ${this.lastProcessedBlock} to START_BLOCK ${this.startBlock}`);
                this.lastProcessedBlock = Math.max(0, this.startBlock - 1); // Start from START_BLOCK
            }
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error loading progress:`, error);
            this.lastProcessedBlock = Math.max(0, this.startBlock - 1);
            this.processedBlocks.clear();
        }
    }

    /**
     * Load progress from dedicated progress file
     */
    async loadFromProgressFile() {
        try {
            const progressData = await fs.readFile(this.progressFilename, 'utf8');
            const progress = JSON.parse(progressData);
            
            this.lastProcessedBlock = progress.lastProcessedBlock || 0;
            this.processedBlocks = new Set(progress.processedBlocks || []);
            this.initialScanCompleted = progress.initialScanCompleted || false;
            
            // Remove any processed blocks that are below START_BLOCK
            const filteredBlocks = Array.from(this.processedBlocks).filter(block => block >= this.startBlock);
            this.processedBlocks = new Set(filteredBlocks);
            
            console.log(`[${new Date().toISOString()}] INFO: 📊 Loaded from progress file: last block ${this.lastProcessedBlock}, ${this.processedBlocks.size} blocks processed (>= ${this.startBlock})`);
            
        } catch (error) {
            // Progress file doesn't exist or is corrupted
            console.log(`[${new Date().toISOString()}] INFO: 📊 No progress file found, will analyze CSV`);
            this.lastProcessedBlock = 0;
            this.processedBlocks.clear();
        }
    }

    /**
     * Load progress from CSV file (fallback method) - RESPECTS START_BLOCK
     */
    async loadFromCSV() {
        try {
            const fileContent = await fs.readFile(this.csvFilename, 'utf8');
            const lines = fileContent.trim().split('\n');
            
            if (lines.length <= 1) {
                this.lastProcessedBlock = Math.max(0, this.startBlock - 1);
                console.log(`[${new Date().toISOString()}] INFO: 📊 Empty CSV file - will start from block ${this.startBlock}`);
                return;
            }

            // Analyze all blocks in CSV to find max and build processed set
            let maxBlockNumber = 0;
            const processedBlocksFromCSV = new Set();
            
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (line) {
                    const columns = line.split(',');
                    if (columns.length > 0) {
                        const blockNumber = parseInt(columns[0]);
                        if (!isNaN(blockNumber) && blockNumber >= this.startBlock) { // Only count blocks >= START_BLOCK
                            maxBlockNumber = Math.max(maxBlockNumber, blockNumber);
                            processedBlocksFromCSV.add(blockNumber);
                        }
                    }
                }
            }
            
            this.lastProcessedBlock = Math.max(maxBlockNumber, this.startBlock - 1);
            this.processedBlocks = processedBlocksFromCSV;
            
            console.log(`[${new Date().toISOString()}] INFO: 📊 Analyzed CSV: max block ${this.lastProcessedBlock}, ${this.processedBlocks.size} unique blocks processed (>= ${this.startBlock})`);
            
            // Save this progress to progress file for next time
            await this.saveProgress();
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error analyzing CSV:`, error);
            this.lastProcessedBlock = Math.max(0, this.startBlock - 1);
            this.processedBlocks.clear();
        }
    }

    /**
     * Save progress to dedicated progress file - INCLUDES START_BLOCK INFO
     */
    async saveProgress() {
        try {
            const progress = {
                lastProcessedBlock: this.lastProcessedBlock,
                processedBlocks: Array.from(this.processedBlocks),
                initialScanCompleted: this.initialScanCompleted,
                startBlock: this.startBlock, // Save START_BLOCK for reference
                lastUpdated: new Date().toISOString(),
                address: this.address
            };
            
            await fs.writeFile(this.progressFilename, JSON.stringify(progress, null, 2), 'utf8');
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error saving progress:`, error);
        }
    }

    /**
     * Perform initial scan - RESPECTS START_BLOCK
     */
    async performInitialScan() {
        try {
            const latestBlock = await this.getLatestBlockNumber();
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Initial scan - Latest block: ${latestBlock}, Last processed: ${this.lastProcessedBlock}, Start block: ${this.startBlock}`);
            
            // Determine the actual starting point
            const actualStartBlock = Math.max(this.startBlock, this.lastProcessedBlock + 1);
            
            if (actualStartBlock > latestBlock) {
                console.log(`[${new Date().toISOString()}] INFO: ✅ Already up to date - no blocks to scan (start: ${actualStartBlock}, latest: ${latestBlock})`);
                this.initialScanCompleted = true;
                await this.saveProgress();
                return;
            }
            
            const totalBlocksToScan = latestBlock - actualStartBlock + 1;
            
            if (this.lastProcessedBlock < this.startBlock) {
                // First time running or starting fresh
                console.log(`[${new Date().toISOString()}] INFO: 🔍 BLOCKCHAIN SCAN - Scanning ${totalBlocksToScan} blocks from START_BLOCK ${actualStartBlock} to ${latestBlock}`);
                console.log(`[${new Date().toISOString()}] INFO: ⚡ Skipping ${this.startBlock - 1} early blocks (before START_BLOCK)`);
            } else {
                // Resume from where we left off
                console.log(`[${new Date().toISOString()}] INFO: 🔍 RESUMING SCAN - Catching up on ${totalBlocksToScan} blocks from ${actualStartBlock} to ${latestBlock}`);
            }
            
            await this.scanBlockRangeParallel(actualStartBlock, latestBlock);
            
            this.initialScanCompleted = true;
            await this.saveProgress();
            console.log(`[${new Date().toISOString()}] INFO: ✅ Initial scan completed`);
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error during initial scan:`, error);
        }
    }

    /**
     * Enhanced parallel scanning with START_BLOCK respect
     */
    async scanBlockRangeParallel(fromBlock, toBlock) {
        // Ensure we never scan below START_BLOCK
        const actualFromBlock = Math.max(fromBlock, this.startBlock);
        
        if (actualFromBlock > toBlock) {
            console.log(`[${new Date().toISOString()}] INFO: ⏭️ No blocks to scan (adjusted range: ${actualFromBlock} to ${toBlock})`);
            return;
        }
        
        const totalBlocks = toBlock - actualFromBlock + 1;
        console.log(`[${new Date().toISOString()}] INFO: ⚡ Parallel scanning ${totalBlocks} blocks from ${actualFromBlock} to ${toBlock}`);
        
        if (actualFromBlock > fromBlock) {
            const skippedBlocks = actualFromBlock - fromBlock;
            console.log(`[${new Date().toISOString()}] INFO: ⏭️ Skipped ${skippedBlocks} blocks before START_BLOCK (${fromBlock} to ${actualFromBlock - 1})`);
        }
        
        let processedBlocks = 0;
        let totalTransactionsFound = 0;
        const startTime = Date.now();
        
        for (let batchStart = actualFromBlock; batchStart <= toBlock; batchStart += this.batchSize) {
            const batchEnd = Math.min(batchStart + this.batchSize - 1, toBlock);
            const batchBlocks = [];
            
            // Only process blocks that haven't been processed yet
            for (let i = batchStart; i <= batchEnd; i++) {
                if (!this.processedBlocks.has(i)) {
                    batchBlocks.push(i);
                }
            }
            
            if (batchBlocks.length === 0) {
                console.log(`[${new Date().toISOString()}] INFO: ⏭️ Skipping batch ${batchStart}-${batchEnd} (already processed)`);
                processedBlocks += (batchEnd - batchStart + 1);
                continue;
            }
            
            console.log(`[${new Date().toISOString()}] INFO: 📊 Processing batch: ${batchBlocks.length} new blocks from ${batchStart} to ${batchEnd}`);
            
            const chunks = this.chunkArray(batchBlocks, this.maxConcurrentRequests);
            const allBatchTransactions = [];
            
            for (const chunk of chunks) {
                const chunkPromises = chunk.map(blockNum => this.processBlockParallel(blockNum));
                const chunkResults = await Promise.allSettled(chunkPromises);
                
                for (const result of chunkResults) {
                    if (result.status === 'fulfilled' && result.value) {
                        const { blockNum, transactions } = result.value;
                        
                        // Mark block as processed
                        this.processedBlocks.add(blockNum);
                        this.lastProcessedBlock = Math.max(this.lastProcessedBlock, blockNum);
                        
                        if (transactions.length > 0) {
                            console.log(`[${new Date().toISOString()}] INFO: 🎯 Found ${transactions.length} relevant transaction(s) in block ${blockNum}`);
                            allBatchTransactions.push(...transactions);
                            totalTransactionsFound += transactions.length;
                        }
                        
                        processedBlocks++;
                    }
                }
                
                if (this.requestDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, this.requestDelay));
                }
            }
            
            // Save transactions and progress after each batch
            if (allBatchTransactions.length > 0) {
                await this.saveTransactionsToCSV(allBatchTransactions);
                console.log(`[${new Date().toISOString()}] INFO: 💾 Saved batch of ${allBatchTransactions.length} transactions`);
            }
            
            // Save progress periodically
            await this.saveProgress();
            
            // Progress reporting
            const progress = ((processedBlocks / totalBlocks) * 100).toFixed(2);
            const elapsed = (Date.now() - startTime) / 1000;
            const blocksPerSecond = processedBlocks / elapsed;
            const estimatedTimeRemaining = (totalBlocks - processedBlocks) / blocksPerSecond;
            
            console.log(`[${new Date().toISOString()}] INFO: 📊 Progress: ${progress}% (${processedBlocks}/${totalBlocks} blocks)`);
            console.log(`[${new Date().toISOString()}] INFO: ⚡ Speed: ${blocksPerSecond.toFixed(2)} blocks/sec, ETA: ${Math.round(estimatedTimeRemaining / 60)} minutes`);
            console.log(`[${new Date().toISOString()}] INFO: 💎 Total transactions found: ${totalTransactionsFound}`);
        }
        
        const totalTime = (Date.now() - startTime) / 1000;
        console.log(`[${new Date().toISOString()}] INFO: ✅ Completed parallel scanning ${totalBlocks} blocks in ${Math.round(totalTime)} seconds`);
        console.log(`[${new Date().toISOString()}] INFO: 🎯 Total transactions found: ${totalTransactionsFound}`);
    }

    /**
     * Process a single block in parallel
     */
    async processBlockParallel(blockNumber) {
        try {
            const block = await this.getBlock(blockNumber);
            
            if (!block) {
                return { blockNum: blockNumber, transactions: [] };
            }
            
            const relevantTxs = this.filterTransactionsForAddress(block);
            const processedTransactions = [];
            
            if (relevantTxs.length > 0) {
                const txPromises = relevantTxs.map(tx => this.processTransaction(tx, block));
                const txResults = await Promise.allSettled(txPromises);
                
                for (const result of txResults) {
                    if (result.status === 'fulfilled' && result.value) {
                        processedTransactions.push(result.value);
                    }
                }
            }
            
            return { blockNum: blockNumber, transactions: processedTransactions };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error processing block ${blockNumber}:`, error.message);
            return { blockNum: blockNumber, transactions: [] };
        }
    }

    // ... (rest of the utility methods remain the same)
    
    chunkArray(array, chunkSize) {
        const chunks = [];
        for (let i = 0; i < array.length; i += chunkSize) {
            chunks.push(array.slice(i, i + chunkSize));
        }
        return chunks;
    }

    async makeRpcCall(method, params = []) {
        const maxRetries = 3;
        let retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                const response = await axios.post(this.rpcUrl, {
                    jsonrpc: '2.0',
                    method: method,
                    params: params,
                    id: 1
                }, {
                    headers: {
                        'Content-Type': 'application/json',
                        'Connection': 'keep-alive'
                    },
                    timeout: 10000,
                    maxRedirects: 0
                });

                if (response.data.error) {
                    throw new Error(`RPC Error: ${response.data.error.message}`);
                }

                return response.data.result;
                
            } catch (error) {
                retryCount++;
                if (retryCount < maxRetries) {
                    const delay = Math.min(100 * retryCount, 500);
                    await new Promise(resolve => setTimeout(resolve, delay));
                } else {
                    throw error;
                }
            }
        }
    }

    async getLatestBlockNumber() {
        const result = await this.makeRpcCall('eth_blockNumber');
        return parseInt(result, 16);
    }

    async getBlock(blockNumber) {
        const hexBlockNumber = '0x' + blockNumber.toString(16);
        return await this.makeRpcCall('eth_getBlockByNumber', [hexBlockNumber, true]);
    }

    async getTransactionReceipt(txHash) {
        return await this.makeRpcCall('eth_getTransactionReceipt', [txHash]);
    }

    hexToDecimal(hex) {
        if (!hex || hex === '0x') return '0';
        return parseInt(hex, 16).toString();
    }

    weiToEther(wei) {
        const weiNum = BigInt(wei || 0);
        const etherNum = Number(weiNum) / Math.pow(10, 18);
        return etherNum.toFixed(18);
    }

    filterTransactionsForAddress(block) {
        const targetAddress = this.address.toLowerCase();
        const relevantTxs = [];
        
        if (!block || !block.transactions) {
            return relevantTxs;
        }

        for (const tx of block.transactions) {
            if (!tx) continue;
            
            const toAddress = (tx.to || '').toLowerCase();
            const fromAddress = (tx.from || '').toLowerCase();
            
            if (toAddress === targetAddress || fromAddress === targetAddress) {
                relevantTxs.push(tx);
            }
        }
        
        return relevantTxs;
    }

    async processTransaction(tx, block) {
        try {
            const receipt = await this.getTransactionReceipt(tx.hash);
            
            const txData = {
                blockNumber: parseInt(block.number, 16),
                transactionHash: tx.hash,
                from: tx.from || '',
                to: tx.to || '',
                value: this.weiToEther(tx.value || '0x0'),
                gasUsed: receipt ? this.hexToDecimal(receipt.gasUsed) : '0',
                gasPrice: this.hexToDecimal(tx.gasPrice || '0x0'),
                timestamp: parseInt(block.timestamp, 16),
                status: receipt ? (receipt.status === '0x1' ? 'success' : 'failed') : 'unknown'
            };
            
            return txData;
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error processing transaction ${tx.hash}:`, error.message);
            return null;
        }
    }

    async saveTransactionsToCSV(transactions) {
        if (transactions.length === 0) return;
        
        try {
            const csvLines = transactions.map(tx => {
                return `${tx.blockNumber},${tx.transactionHash},${tx.from},${tx.to},${tx.value},${tx.gasUsed},${tx.gasPrice},${tx.timestamp},${tx.status}`;
            });
            
            const csvContent = csvLines.join('\n') + '\n';
            await fs.appendFile(this.csvFilename, csvContent, 'utf8');
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error saving transactions to CSV:`, error);
            throw error;
        }
    }

    async ensureCsvFile() {
        try {
            await fs.access(this.csvFilename);
            console.log(`[${new Date().toISOString()}] INFO: 📄 CSV file ${this.csvFilename} exists`);
        } catch (error) {
            console.log(`[${new Date().toISOString()}] INFO: 📝 Creating new CSV file: ${this.csvFilename}`);
            const headers = 'blockNumber,transactionHash,from,to,value,gasUsed,gasPrice,timestamp,status\n';
            await fs.writeFile(this.csvFilename, headers, 'utf8');
        }
    }

    start() {
        if (!this.cronSchedule) {
            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ CRON_SCHEDULE_COUNTER not set, only initial scan performed`);
            return;
        }

        if (this.isRunning) {
            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ TransactionTracker already running`);
            return;
        }

        console.log(`[${new Date().toISOString()}] INFO: 🕒 Starting TransactionTracker scheduled updates with schedule: ${this.cronSchedule}`);
        
        cron.schedule(this.cronSchedule, async () => {
            if (this.initialScanCompleted) {
                await this.updateTransactions();
            } else {
                console.log(`[${new Date().toISOString()}] INFO: ⏳ Skipping scheduled update - initial scan still in progress`);
            }
        });

        this.isRunning = true;
    }

    stop() {
        if (this.isRunning) {
            this.isRunning = false;
            console.log(`[${new Date().toISOString()}] INFO: 🛑 TransactionTracker stopped`);
        }
    }

    async updateTransactions() {
        try {
            console.log(`[${new Date().toISOString()}] INFO: 🔍 Starting scheduled transaction update for address: ${this.address}`);
            
            const latestBlock = await this.getLatestBlockNumber();
            console.log(`[${new Date().toISOString()}] INFO: 📊 Latest block: ${latestBlock}, Last processed: ${this.lastProcessedBlock}`);
            
            const startBlock = Math.max(this.lastProcessedBlock + 1, this.startBlock);
            
            if (startBlock > latestBlock) {
                console.log(`[${new Date().toISOString()}] INFO: ✅ No new blocks to process`);
                return;
            }
            
            const blocksToProcess = Math.min(latestBlock - startBlock + 1, 100);
            const endBlock = startBlock + blocksToProcess - 1;
            
            console.log(`[${new Date().toISOString()}] INFO: 🔄 Processing blocks ${startBlock} to ${endBlock} (${blocksToProcess} blocks)`);
            
            await this.scanBlockRangeParallel(startBlock, endBlock);
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error updating transactions:`, error);
        }
    }

    async getStatistics() {
        try {
            const fileContent = await fs.readFile(this.csvFilename, 'utf8');
            const lines = fileContent.trim().split('\n');
            
            if (lines.length <= 1) {
                return {
                    totalTransactions: 0,
                    lastProcessedBlock: this.lastProcessedBlock,
                    startBlock: this.startBlock,
                    csvFile: this.csvFilename
                };
            }
            
            const transactions = lines.slice(1);
            let totalValue = 0;
            let successfulTxs = 0;
            
            for (const line of transactions) {
                const columns = line.split(',');
                if (columns.length >= 9) {
                    totalValue += parseFloat(columns[4]) || 0;
                    if (columns[8] === 'success') {
                        successfulTxs++;
                    }
                }
            }
            
            return {
                totalTransactions: transactions.length,
                successfulTransactions: successfulTxs,
                totalValue: totalValue.toFixed(18),
                lastProcessedBlock: this.lastProcessedBlock,
                startBlock: this.startBlock,
                processedBlocksCount: this.processedBlocks.size,
                csvFile: this.csvFilename,
                progressFile: this.progressFilename,
                address: this.address,
                initialScanCompleted: this.initialScanCompleted
            };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Error getting statistics:`, error);
            return null;
        }
    }

    async manualUpdate() {
        console.log(`[${new Date().toISOString()}] INFO: 🔧 Manual transaction update triggered`);
        await this.updateTransactions();
    }
}

// Export functions remain the same
let transactionTracker = null;

async function initializeTransactionTracker() {
    try {
        transactionTracker = new TransactionTracker();
        await transactionTracker.initialize();
        return transactionTracker;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Failed to initialize transaction tracker:`, error);
        throw error;
    }
}

function startTransactionTracking() {
    if (transactionTracker) {
        transactionTracker.start();
    } else {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Transaction tracker not initialized`);
    }
}

function stopTransactionTracking() {
    if (transactionTracker) {
        transactionTracker.stop();
    }
}

async function getTransactionStats() {
    if (transactionTracker) {
        return await transactionTracker.getStatistics();
    }
    return null;
}

async function updateTransactionsManually() {
    if (transactionTracker) {
        await transactionTracker.manualUpdate();
    } else {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Transaction tracker not initialized`);
    }
}

module.exports = {
    TransactionTracker,
    initializeTransactionTracker,
    startTransactionTracking,
    stopTransactionTracking,
    getTransactionStats,
    updateTransactionsManually
};