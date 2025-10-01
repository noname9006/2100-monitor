const fs = require('fs');
const readline = require('readline');
const path = require('path');
const https = require('https');

require('dotenv').config();

class ComprehensiveTransactionAnalyzer {
    constructor(csvFilename) {
        this.csvFilename = csvFilename;
        this.address = this.extractAddressFromFilename(csvFilename);
        this.agentAddress = process.env.AGENT ? process.env.AGENT.toLowerCase() : null;
        this.processedCount = 0;
        this.duplicateTransactions = new Map(); // Track duplicate txids
        this.duplicateCount = 0;
        
        // DEBUG flag - defaults to 0 (off) if not set
        this.DEBUG = process.env.DEBUG === '1';
        
        // Initialize txCountData as empty, will be populated in init()
        this.txCountData = new Map();
        
        // Define thresholds in BTC
        this.THRESHOLDS = {
            ONE_SAT: 0.00000001,
            TEN_SATS: 0.0000001,
            HUNDRED_SATS: 0.000001,
            TOP_UP_MIN: 0.000001
        };
        
        // Time cutoffs (full calendar days in UTC) - EXCLUDE TODAY
        const now = new Date();
        
        // Get yesterday as the most recent complete day
        const yesterday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
        const yesterdayEnd = new Date(yesterday);
        yesterdayEnd.setUTCHours(23, 59, 59, 999);
        
        // Calculate start of day 7 days before yesterday (8 days ago from today)
        const sevenDaysAgo = new Date(yesterday);
        sevenDaysAgo.setUTCDate(yesterday.getUTCDate() - 6); // 7 days total including yesterday
        
        // Calculate start of day 14 days before yesterday (15 days ago from today)
        const fourteenDaysAgo = new Date(yesterday);
        fourteenDaysAgo.setUTCDate(yesterday.getUTCDate() - 13); // 14 days total including yesterday
        
        // Calculate start of day 30 days before yesterday (31 days ago from today)  
        const thirtyDaysAgo = new Date(yesterday);
        thirtyDaysAgo.setUTCDate(yesterday.getUTCDate() - 29); // 30 days total including yesterday
        
        this.timeCutoffs = {
            last7Days: Math.floor(sevenDaysAgo.getTime() / 1000),
            last14Days: Math.floor(fourteenDaysAgo.getTime() / 1000),
            last30Days: Math.floor(thirtyDaysAgo.getTime() / 1000),
            yesterdayEnd: Math.floor(yesterdayEnd.getTime() / 1000)
        };
        
        // Daily analytics for calendar export and breakdown
        this.dailyAnalytics = new Map();
        
        // Store last 14 full days for breakdown
        this.last14DaysBreakdown = new Map();
        
        // Initialize analytics containers
        this.initializeAnalytics();
        
        console.log(`[${new Date().toISOString()}] INFO: # ComprehensiveTransactionAnalyzer initialized for address: ${this.address}`);
        console.log(`[${new Date().toISOString()}] INFO: # CSV file: ${this.csvFilename}`);
        console.log(`[${new Date().toISOString()}] INFO: # Current time: ${now.toISOString()}`);
        console.log(`[${new Date().toISOString()}] INFO: # Most recent complete day: ${yesterday.toISOString().split('T')[0]}`);
        console.log(`[${new Date().toISOString()}] INFO: # Last 7 days cutoff: ${new Date(this.timeCutoffs.last7Days * 1000).toISOString()} (7 complete days)`);
        console.log(`[${new Date().toISOString()}] INFO: # Last 14 days cutoff: ${new Date(this.timeCutoffs.last14Days * 1000).toISOString()} (14 complete days)`);
        console.log(`[${new Date().toISOString()}] INFO: # Last 30 days cutoff: ${new Date(this.timeCutoffs.last30Days * 1000).toISOString()} (30 complete days)`);
        console.log(`[${new Date().toISOString()}] INFO: # Excluding today (${now.toISOString().split('T')[0]}) as incomplete`);
        
        if (this.agentAddress) {
            console.log(`[${new Date().toISOString()}] INFO: # Agent address configured: ${this.agentAddress}`);
        } else {
            console.log(`[${new Date().toISOString()}] INFO: # No agent address configured (AGENT env var not set)`);
        }
        
        // Show DEBUG status
        console.log(`[${new Date().toISOString()}] INFO: # Debug logging: ${this.DEBUG ? 'ENABLED' : 'DISABLED'}`);
    }

    // Debug logging helper
    debugLog(message) {
        if (this.DEBUG) {
            console.log(`[${new Date().toISOString()}] DEBUG: ${message}`);
        }
    }

    // Add this new method for initialization
    async init() {
        // Parse TX_COUNT environment variable
        this.txCountData = await this.parseTxCountData();
        
        if (this.txCountData.size > 0) {
            console.log(`[${new Date().toISOString()}] INFO: # TX_COUNT data loaded for ${this.txCountData.size} days`);
            
            if (this.DEBUG) {
                // DEBUG: Show a few sample dates to verify alignment
                const sampleDates = Array.from(this.txCountData.keys()).slice(0, 3);
                this.debugLog(`# Sample TX_COUNT dates: ${sampleDates.join(', ')}`);
            }
        } else {
            console.log(`[${new Date().toISOString()}] INFO: # No TX_COUNT data available (TX_COUNT env var not set)`);
        }
    }

    // Helper function to make HTTPS requests
    httpsGet(url) {
        return new Promise((resolve, reject) => {
            https.get(url, (res) => {
                let data = '';
                res.on('data', (chunk) => {
                    data += chunk;
                });
                res.on('end', () => {
                    try {
                        resolve(JSON.parse(data));
                    } catch (error) {
                        reject(new Error(`Failed to parse JSON: ${error.message}`));
                    }
                });
            }).on('error', (error) => {
                reject(error);
            });
        });
    }

    async parseTxCountData() {
        const txCountString = process.env.TX_COUNT;
        const txCountData = new Map();
        
        if (!txCountString) {
            return txCountData;
        }
        
        try {
            // Check if TX_COUNT is a URL
            if (txCountString.startsWith('http')) {
                console.log(`[${new Date().toISOString()}] INFO: # Fetching TX_COUNT data from API: ${txCountString}`);
                
                // Fetch data from API using built-in https module
                const apiData = await this.httpsGet(txCountString);
                
                // Parse API response format: [["2025-09-23T00:00:00.000Z","46814"], ...]
                for (const entry of apiData) {
                    if (Array.isArray(entry) && entry.length >= 2) {
                        const dateStr = entry[0];
                        const count = parseInt(entry[1]);
                        
                        // Use the date as-is for proper alignment
                        const date = new Date(dateStr);
                        const dayKey = date.toISOString().split('T')[0];
                        
                        txCountData.set(dayKey, count);
                    }
                }
            } else {
                // Handle raw data format: "2025-09-23T00:00:00.000Z","46814","2025-09-22T00:00:00.000Z","45821"
                const entries = txCountString.split(',');
                for (const entry of entries) {
                    const trimmed = entry.trim();
                    if (!trimmed) continue;
                    
                    // Extract date and count from format like "2025-09-23T00:00:00.000Z","46814"
                    const match = trimmed.match(/"([^"]+)","(\d+)"/);
                    if (match) {
                        const dateStr = match[1];
                        const count = parseInt(match[2]);
                        
                        // Use the date as-is for proper alignment
                        const date = new Date(dateStr);
                        const dayKey = date.toISOString().split('T')[0];
                        
                        txCountData.set(dayKey, count);
                    }
                }
            }
            
            console.log(`[${new Date().toISOString()}] INFO: # Parsed TX_COUNT data for ${txCountData.size} days`);
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] WARN: # Error parsing TX_COUNT data: ${error.message}`);
        }
        
        return txCountData;
    }

    // Better period calculation with proper date handling
    getTotalTxCountForPeriod(startTimestamp, endTimestamp) {
        let total = 0;
        const startDate = new Date(startTimestamp * 1000);
        const endDate = new Date(endTimestamp * 1000);
        
        // Convert to date strings for comparison
        const startDateStr = startDate.toISOString().split('T')[0];
        const endDateStr = endDate.toISOString().split('T')[0];
        
        this.debugLog(`# TX_COUNT period: ${startDateStr} to ${endDateStr}`);
        
        for (const [dayKey, count] of this.txCountData) {
            // Use string comparison for date ranges (YYYY-MM-DD format)
            if (dayKey >= startDateStr && dayKey <= endDateStr) {
                total += count;
                this.debugLog(`# Including ${dayKey}: ${count} txs`);
            }
        }
        
        this.debugLog(`# Total TX_COUNT for period: ${total}`);
        return total;
    }

    getTxCountForDay(dayKey) {
        return this.txCountData.get(dayKey) || 0;
    }

    calculateSharePercentage(ourCount, totalCount) {
        if (totalCount === 0) return '0.00';
        const percentage = ((ourCount / totalCount) * 100);
        
        if (this.DEBUG) {
            this.debugLog(`# Share calc: ${ourCount}/${totalCount} = ${percentage.toFixed(2)}%`);
        }
        
        return percentage.toFixed(2);
    }

    extractAddressFromFilename(filename) {
        const baseName = path.basename(filename, '.csv');
        return baseName.toLowerCase();
    }

    initializeAnalytics() {
        this.analytics = {
            allTime: this.createEmptyStats(),
            last30Days: this.createEmptyStats(),
            last14Days: this.createEmptyStats(),
            last7Days: this.createEmptyStats()
        };
    }

    createEmptyStats() {
        return {
            // Incoming transaction categories
            satWheel: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map(),
                oneSatTx: 0,
                tenSatsTx: 0,
                // Agent tracking for 1 sat only
                oneSatUserTx: 0,
                oneSatAgentTx: 0,
                userAmount: 0,
                agentAmount: 0
            },
            guessTheBlock: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map()
            },
            topUps: {
                totalOps: 0,
                totalAmount: 0,
                largestTopUp: 0,
                smallestTopUp: Infinity,
                transactions: []
            },
            // Track uncategorized incoming transactions
            uncategorizedIncoming: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map(),
                valueRanges: new Map() // Track value distribution only
            },
            // Total incoming tracking
            allIncoming: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map()
            },
            gamingIncoming: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map()
            },
            // Outgoing transaction stats
            payouts: {
                totalTx: 0,
                totalAmount: 0,
                wallets: new Map(),
                largestPayout: 0,
                smallestPayout: Infinity
            },
            allOutgoing: {
                totalTx: 0,
                totalAmount: 0
            },
            // Additional stats
            totalTransactions: 0,
            earliestTimestamp: Infinity,
            latestTimestamp: 0,
            blockRange: {
                min: Infinity,
                max: 0
            }
        };
    }

    // Check for duplicate transaction IDs before processing
    async checkForDuplicates() {
        return new Promise((resolve, reject) => {
            try {
                console.log(`[${new Date().toISOString()}] INFO: # Checking for duplicate transaction IDs...`);
                
                const fileStream = fs.createReadStream(this.csvFilename);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });

                const txidSet = new Set();
                const duplicates = new Map();
                let isFirstLine = true;
                let lineCount = 0;
                let totalTxids = 0;

                rl.on('line', (line) => {
                    try {
                        lineCount++;
                        
                        if (isFirstLine) {
                            isFirstLine = false;
                            return;
                        }

                        const trimmedLine = line.trim();
                        if (!trimmedLine) return;

                        const columns = trimmedLine.split(',');
                        if (columns.length >= 2) {
                            const txid = columns[1]; // Transaction hash is in column 1
                            totalTxids++;
                            
                            if (txidSet.has(txid)) {
                                if (!duplicates.has(txid)) {
                                    duplicates.set(txid, []);
                                }
                                duplicates.get(txid).push(lineCount);
                            } else {
                                txidSet.add(txid);
                            }
                        }
                    } catch (error) {
                        console.warn(`[${new Date().toISOString()}] WARN: # Error checking line ${lineCount}: ${error.message}`);
                    }
                });

                rl.on('close', () => {
                    this.duplicateTransactions = duplicates;
                    this.duplicateCount = Array.from(duplicates.values()).reduce((sum, lines) => sum + lines.length, 0);
                    
                    console.log(`[${new Date().toISOString()}] INFO: # Duplicate check complete:`);
                    console.log(`[${new Date().toISOString()}] INFO: # Total transaction IDs checked: ${totalTxids.toLocaleString()}`);
                    console.log(`[${new Date().toISOString()}] INFO: # Unique transaction IDs: ${txidSet.size.toLocaleString()}`);
                    console.log(`[${new Date().toISOString()}] INFO: # Duplicate transaction IDs found: ${duplicates.size.toLocaleString()}`);
                    console.log(`[${new Date().toISOString()}] INFO: # Total duplicate occurrences: ${this.duplicateCount.toLocaleString()}`);
                    
                    if (duplicates.size > 0) {
                        console.log(`[${new Date().toISOString()}] WARN: # DUPLICATE TRANSACTIONS DETECTED:`);
                        let reportCount = 0;
                        for (const [txid, lines] of duplicates.entries()) {
                            if (reportCount < 10) { // Show first 10 duplicates
                                console.log(`[${new Date().toISOString()}] WARN: # TXID: ${txid} appears on lines: ${lines.join(', ')}`);
                                reportCount++;
                            }
                        }
                        if (duplicates.size > 10) {
                            console.log(`[${new Date().toISOString()}] WARN: # ... and ${duplicates.size - 10} more duplicate transaction IDs`);
                        }
                    } else {
                        console.log(`[${new Date().toISOString()}] INFO: # ✓ No duplicate transaction IDs found`);
                    }
                    
                    resolve();
                });

                rl.on('error', (error) => {
                    console.error(`[${new Date().toISOString()}] ERROR: # Error checking for duplicates:`, error);
                    reject(error);
                });

            } catch (error) {
                console.error(`[${new Date().toISOString()}] ERROR: # Error setting up duplicate check:`, error);
                reject(error);
            }
        });
    }

    getDayKey(timestamp) {
        const date = new Date(timestamp * 1000);
        return date.toISOString().split('T')[0]; // YYYY-MM-DD format
    }

    getWeekdayName(dateString) {
        const date = new Date(dateString + 'T00:00:00.000Z');
        const weekdays = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
        return weekdays[date.getUTCDay()];
    }

    // Updated function to calculate period averages with proper period mapping
    calculatePeriodAverages(periodStats, dayCount) {
        if (dayCount === 0) return null;
        
        // Get yesterday as the most recent complete day
        const now = new Date();
        const yesterday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
        
        const cutoffDate = new Date(yesterday);
        cutoffDate.setUTCDate(yesterday.getUTCDate() - (dayCount - 1)); // Include yesterday in the count
        const cutoffTimestamp = Math.floor(cutoffDate.getTime() / 1000);
        
        this.debugLog(`# Calculating ${dayCount}-day averages from ${cutoffDate.toISOString()} to ${yesterday.toISOString()} (excluding today)`);
        
        // Filter daily analytics to only include complete days within the period
        const relevantDays = Array.from(this.dailyAnalytics.entries())
            .filter(([dateStr, stats]) => {
                const dayDate = new Date(dateStr + 'T00:00:00.000Z');
                const dayTimestamp = Math.floor(dayDate.getTime() / 1000);
                const yesterdayTimestamp = Math.floor(yesterday.getTime() / 1000);
                
                // Must be within period and not be today
                return dayTimestamp >= cutoffTimestamp && dayTimestamp <= yesterdayTimestamp;
            });
        
        if (relevantDays.length === 0) {
            this.debugLog(`# No relevant complete days found for ${dayCount}-day period`);
            return null;
        }
        
        const actualDays = relevantDays.length;
        this.debugLog(`# Found ${actualDays} complete days for ${dayCount}-day period (excluding today)`);
        
        // Calculate totals from daily data
        let totalTransactions = 0;
        let totalSatWheelTx = 0;
        let totalUserTx = 0;
        let totalAgentTx = 0;
        let totalGuessTheBlock = 0;
        let totalTxCountData = 0;
        
        const allUniqueIncomingWallets = new Set(); // Only incoming wallets
        
        relevantDays.forEach(([dateStr, dayStats]) => {
            totalTransactions += dayStats.totalTransactions;
            totalSatWheelTx += dayStats.satWheel.totalTx;
            totalUserTx += (dayStats.satWheel.oneSatUserTx + dayStats.satWheel.tenSatsTx);
            totalAgentTx += dayStats.satWheel.oneSatAgentTx;
            totalGuessTheBlock += dayStats.guessTheBlock.totalTx;
            
            // Add TX_COUNT data for this day
            const dayTxCount = this.getTxCountForDay(dateStr);
            totalTxCountData += dayTxCount;
            
            if (this.DEBUG) {
                this.debugLog(`# Complete day ${dateStr}: Our=${dayStats.totalTransactions}, Network=${dayTxCount}`);
            }
            
            // Only collect wallets that send TO the game (incoming only)
            dayStats.allIncoming.wallets.forEach((_, wallet) => allUniqueIncomingWallets.add(wallet));
        });
        
        // Calculate averages
        const avgTransactions = Math.round(totalTransactions / actualDays);
        const avgUniqueWallets = Math.round(allUniqueIncomingWallets.size / actualDays);
        const avgSatWheelTx = Math.round(totalSatWheelTx / actualDays);
        const avgUserTx = Math.round(totalUserTx / actualDays);
        const avgAgentTx = Math.round(totalAgentTx / actualDays);
        const avgGuessTheBlock = Math.round(totalGuessTheBlock / actualDays);
        const avgTxCountData = Math.round(totalTxCountData / actualDays);
        
        // Calculate share percentage
        const sharePercentage = this.calculateSharePercentage(totalTransactions, totalTxCountData);
        
        this.debugLog(`# ${dayCount}-day totals (complete days only): Our=${totalTransactions}, Network=${totalTxCountData}, Share=${sharePercentage}%`);
        
        return {
            avgTransactions,
            avgUniqueWallets,
            avgSatWheelTx,
            avgUserTx,
            avgAgentTx,
            avgGuessTheBlock,
            avgTxCountData,
            sharePercentage,
            actualDays // Include actual days counted for debugging
        };
    }

    updateDailyStats(tx) {
        const dayKey = this.getDayKey(tx.timestamp);
        
        if (!this.dailyAnalytics.has(dayKey)) {
            this.dailyAnalytics.set(dayKey, this.createEmptyStats());
        }
        
        const dailyStats = this.dailyAnalytics.get(dayKey);
        this.processDailyTransaction(tx, dailyStats);
        
        // Also track for last 14 days breakdown if within range
        if (tx.timestamp >= this.timeCutoffs.last14Days) {
            if (!this.last14DaysBreakdown.has(dayKey)) {
                this.last14DaysBreakdown.set(dayKey, this.createEmptyStats());
            }
            const breakdownStats = this.last14DaysBreakdown.get(dayKey);
            this.processDailyTransaction(tx, breakdownStats);
        }
    }

    processDailyTransaction(tx, stats) {
        const isIncoming = tx.to === this.address;
        const isOutgoing = tx.from === this.address;
        
        if (!isIncoming && !isOutgoing) return;
        if (tx.value <= 0 || tx.value < 1e-18) return;

        // Update general stats
        stats.totalTransactions++;
        stats.earliestTimestamp = Math.min(stats.earliestTimestamp, tx.timestamp);
        stats.latestTimestamp = Math.max(stats.latestTimestamp, tx.timestamp);
        stats.blockRange.min = Math.min(stats.blockRange.min, tx.blockNumber);
        stats.blockRange.max = Math.max(stats.blockRange.max, tx.blockNumber);
        
        if (isIncoming) {
            this.processIncomingTransaction(tx, stats);
        } else if (isOutgoing) {
            this.processOutgoingTransaction(tx, stats);
        }
    }

    processTransaction(tx) {
        // Determine if transaction is incoming or outgoing
        const isIncoming = tx.to === this.address;
        const isOutgoing = tx.from === this.address;
        
        if (!isIncoming && !isOutgoing) return;

        // Filter out zero-value transactions
        if (tx.value <= 0 || tx.value < 1e-18) return;

        // NOTE: Date filtering is now handled in analyzeStreaming()

        // Update daily analytics
        this.updateDailyStats(tx);

        // Process for all time periods
        const periods = ['allTime'];
        if (tx.timestamp >= this.timeCutoffs.last30Days) periods.push('last30Days');
        if (tx.timestamp >= this.timeCutoffs.last14Days) periods.push('last14Days');
        if (tx.timestamp >= this.timeCutoffs.last7Days) periods.push('last7Days');

        for (const period of periods) {
            const stats = this.analytics[period];
            
            // Update general stats
            stats.totalTransactions++;
            stats.earliestTimestamp = Math.min(stats.earliestTimestamp, tx.timestamp);
            stats.latestTimestamp = Math.max(stats.latestTimestamp, tx.timestamp);
            stats.blockRange.min = Math.min(stats.blockRange.min, tx.blockNumber);
            stats.blockRange.max = Math.max(stats.blockRange.max, tx.blockNumber);
            
            if (isIncoming) {
                this.processIncomingTransaction(tx, stats);
            } else if (isOutgoing) {
                this.processOutgoingTransaction(tx, stats);
            }
        }
    }

    processIncomingTransaction(tx, stats) {
        // ALWAYS track in all incoming first
        stats.allIncoming.totalTx++;
        stats.allIncoming.totalAmount += tx.value;
        this.updateWalletMap(stats.allIncoming.wallets, tx.from, tx.value);

        // Check if transaction is from agent
        const isFromAgent = this.agentAddress && tx.from === this.agentAddress;

        // Categorize EVERY transaction precisely
        if (this.isFloatEqual(tx.value, this.THRESHOLDS.ONE_SAT)) {
            // Exactly 1 Sat
            stats.satWheel.totalTx++;
            stats.satWheel.totalAmount += tx.value;
            stats.satWheel.oneSatTx++;
            this.updateWalletMap(stats.satWheel.wallets, tx.from, tx.value);
            
            // Agent/User breakdown for 1 sat only
            if (isFromAgent) {
                stats.satWheel.oneSatAgentTx++;
                stats.satWheel.agentAmount += tx.value;
            } else {
                stats.satWheel.oneSatUserTx++;
                stats.satWheel.userAmount += tx.value;
            }
            
            stats.gamingIncoming.totalTx++;
            stats.gamingIncoming.totalAmount += tx.value;
            this.updateWalletMap(stats.gamingIncoming.wallets, tx.from, tx.value);
            
        } else if (this.isFloatEqual(tx.value, this.THRESHOLDS.TEN_SATS)) {
            // Exactly 10 Sats (no agent tracking for 10 sats)
            stats.satWheel.totalTx++;
            stats.satWheel.totalAmount += tx.value;
            stats.satWheel.tenSatsTx++;
            this.updateWalletMap(stats.satWheel.wallets, tx.from, tx.value);
            
            // All 10 sat transactions go to user amount (no agent breakdown)
            stats.satWheel.userAmount += tx.value;
            
            stats.gamingIncoming.totalTx++;
            stats.gamingIncoming.totalAmount += tx.value;
            this.updateWalletMap(stats.gamingIncoming.wallets, tx.from, tx.value);
            
        } else if (this.isFloatEqual(tx.value, this.THRESHOLDS.HUNDRED_SATS)) {
            // Exactly 100 Sats (Guess the Block)
            stats.guessTheBlock.totalTx++;
            stats.guessTheBlock.totalAmount += tx.value;
            this.updateWalletMap(stats.guessTheBlock.wallets, tx.from, tx.value);
            
            stats.gamingIncoming.totalTx++;
            stats.gamingIncoming.totalAmount += tx.value;
            this.updateWalletMap(stats.gamingIncoming.wallets, tx.from, tx.value);
            
        } else if (tx.value > this.THRESHOLDS.TOP_UP_MIN) {
            // Top Ups (> 100 sats)
            stats.topUps.totalOps++;
            stats.topUps.totalAmount += tx.value;
            stats.topUps.largestTopUp = Math.max(stats.topUps.largestTopUp, tx.value);
            if (stats.topUps.smallestTopUp === Infinity) {
                stats.topUps.smallestTopUp = tx.value;
            } else {
                stats.topUps.smallestTopUp = Math.min(stats.topUps.smallestTopUp, tx.value);
            }
            
            stats.topUps.transactions.push({
                hash: tx.transactionHash,
                from: tx.from,
                value: tx.value,
                valueSats: Math.round(tx.value * 100000000),
                timestamp: tx.timestamp,
                blockNumber: tx.blockNumber,
                date: new Date(tx.timestamp * 1000).toISOString().replace('T', ' ').replace('Z', ' UTC')
            });
            
        } else {
            // UNCATEGORIZED transactions (between 0 and 100 sats, but not exact matches)
            stats.uncategorizedIncoming.totalTx++;
            stats.uncategorizedIncoming.totalAmount += tx.value;
            this.updateWalletMap(stats.uncategorizedIncoming.wallets, tx.from, tx.value);
            
            // Track value ranges for analysis only
            const satsValue = Math.round(tx.value * 100000000);
            const rangeKey = this.getValueRange(satsValue);
            if (!stats.uncategorizedIncoming.valueRanges.has(rangeKey)) {
                stats.uncategorizedIncoming.valueRanges.set(rangeKey, { count: 0, totalValue: 0 });
            }
            const range = stats.uncategorizedIncoming.valueRanges.get(rangeKey);
            range.count++;
            range.totalValue += tx.value;
        }
    }

    getValueRange(sats) {
        if (sats === 0) return '0 sats';
        if (sats < 1) return '< 1 sat';
        if (sats < 10) return '1-9 sats';
        if (sats < 100) return '10-99 sats';
        if (sats < 1000) return '100-999 sats';
        if (sats < 10000) return '1k-9.9k sats';
        if (sats < 100000) return '10k-99.9k sats';
        return '100k+ sats';
    }

    processOutgoingTransaction(tx, stats) {
        stats.payouts.totalTx++;
        stats.payouts.totalAmount += tx.value;
        this.updateWalletMap(stats.payouts.wallets, tx.to, tx.value);

        stats.payouts.largestPayout = Math.max(stats.payouts.largestPayout, tx.value);
        if (stats.payouts.smallestPayout === Infinity) {
            stats.payouts.smallestPayout = tx.value;
        } else {
            stats.payouts.smallestPayout = Math.min(stats.payouts.smallestPayout, tx.value);
        }

        stats.allOutgoing.totalTx++;
        stats.allOutgoing.totalAmount += tx.value;
    }

    updateWalletMap(walletMap, address, value) {
        if (!walletMap.has(address)) {
            walletMap.set(address, { count: 0, totalValue: 0, firstSeen: Date.now(), lastSeen: Date.now() });
        }
        const wallet = walletMap.get(address);
        wallet.count++;
        wallet.totalValue += value;
        wallet.lastSeen = Date.now();
    }

    isFloatEqual(a, b, tolerance = 1e-10) {
        return Math.abs(a - b) < tolerance;
    }

    // Fixed analyzeStreaming method with proper exclusion counting
    async analyzeStreaming() {
        // First check for duplicates
        await this.checkForDuplicates();
        
        return new Promise((resolve, reject) => {
            try {
                console.log(`[${new Date().toISOString()}] INFO: # Starting comprehensive streaming analysis (excluding incomplete days)...`);
                
                const fileStream = fs.createReadStream(this.csvFilename);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });

                let isFirstLine = true;
                let lineCount = 0;
                let errorCount = 0;
                let excludedCount = 0;
                let totalRelevantTransactions = 0; // Track transactions relevant to our address

                rl.on('line', (line) => {
                    try {
                        lineCount++;
                        
                        if (isFirstLine) {
                            isFirstLine = false;
                            console.log(`[${new Date().toISOString()}] INFO: # CSV Header: ${line}`);
                            return;
                        }

                        const trimmedLine = line.trim();
                        if (!trimmedLine) return;

                        const columns = trimmedLine.split(',');
                        if (columns.length >= 9) {
                            const tx = {
                                blockNumber: parseInt(columns[0]),
                                transactionHash: columns[1],
                                from: columns[2].toLowerCase(),
                                to: columns[3].toLowerCase(),
                                value: parseFloat(columns[4]),
                                gasUsed: parseInt(columns[5]) || 0,
                                gasPrice: parseInt(columns[6]) || 0,
                                timestamp: parseInt(columns[7]),
                                status: columns[8]
                            };
                            
                            if (tx.status === 'success' && !isNaN(tx.value) && !isNaN(tx.timestamp) && !isNaN(tx.blockNumber)) {
                                // Check if transaction is relevant to our address
                                const isRelevant = (tx.to === this.address || tx.from === this.address);
                                
                                if (isRelevant) {
                                    totalRelevantTransactions++;
                                    
                                    // Check if transaction is from today (incomplete day)
                                    const isFromToday = tx.timestamp > this.timeCutoffs.yesterdayEnd;
                                    
                                    if (isFromToday) {
                                        excludedCount++;
                                        this.debugLog(`# Excluding transaction from incomplete day: ${new Date(tx.timestamp * 1000).toISOString()}`);
                                    } else {
                                        // Process the transaction (it's from a complete day)
                                        this.processTransaction(tx);
                                        this.processedCount++;
                                    }
                                }
                            }
                        }

                        if (lineCount % 100000 === 0) {
                            console.log(`[${new Date().toISOString()}] INFO: # Processed ${lineCount.toLocaleString()} lines, ${this.processedCount.toLocaleString()} valid transactions, ${excludedCount.toLocaleString()} excluded (incomplete day)`);
                            
                            if (global.gc) {
                                global.gc();
                            }
                        }

                    } catch (error) {
                        errorCount++;
                        if (errorCount <= 10) {
                            console.warn(`[${new Date().toISOString()}] WARN: # Error parsing line ${lineCount}: ${error.message}`);
                        }
                    }
                });

                rl.on('close', () => {
                    console.log(`[${new Date().toISOString()}] INFO: # Successfully processed ${this.processedCount.toLocaleString()} valid transactions from ${lineCount.toLocaleString()} lines`);
                    console.log(`[${new Date().toISOString()}] INFO: # Total relevant transactions: ${totalRelevantTransactions.toLocaleString()}`);
                    if (excludedCount > 0) {
                        console.log(`[${new Date().toISOString()}] INFO: # Excluded ${excludedCount.toLocaleString()} transactions from incomplete day (today)`);
                    } else {
                        console.log(`[${new Date().toISOString()}] INFO: # No transactions from incomplete day found`);
                    }
                    if (errorCount > 0) {
                        console.log(`[${new Date().toISOString()}] WARN: # Encountered ${errorCount} parsing errors`);
                    }
                    if (this.duplicateCount > 0) {
                        console.log(`[${new Date().toISOString()}] WARN: # Note: ${this.duplicateCount} duplicate transactions were found but all lines were processed`);
                    }
                    resolve();
                });

                rl.on('error', (error) => {
                    console.error(`[${new Date().toISOString()}] ERROR: # Error reading file:`, error);
                    reject(error);
                });

            } catch (error) {
                console.error(`[${new Date().toISOString()}] ERROR: # Error setting up file stream:`, error);
                reject(error);
            }
        });
    }

    calculateWalletAverages(walletMap, totalTx, totalValue) {
        const uniqueWallets = walletMap.size;
        const avgTxPerWallet = uniqueWallets > 0 ? totalTx / uniqueWallets : 0;
        const avgValuePerWallet = uniqueWallets > 0 ? totalValue / uniqueWallets : 0;
        const avgSatsPerWallet = avgValuePerWallet * 100000000;

        return {
            uniqueWallets,
            avgTxPerWallet,
            avgValuePerWallet,
            avgSatsPerWallet
        };
    }

    formatBTC(value) {
        if (value === 0) return '0';
        if (value === Infinity) return 'N/A';
        return value.toFixed(18).replace(/\.?0+$/, '');
    }

    formatSats(btcValue) {
        if (btcValue === Infinity) return 'N/A';
        return Math.round(btcValue * 100000000);
    }

    formatDate(timestamp) {
        if (timestamp === Infinity || timestamp === 0) return 'N/A';
        return new Date(timestamp * 1000).toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');
    }

    logTopUpDetails(topUps) {
        if (topUps.transactions.length === 0) {
            console.log(`   No top-ups found.`);
            return;
        }

        const sortedTopUps = topUps.transactions.sort((a, b) => b.timestamp - a.timestamp);

        console.log(`\n   # DETAILED TOP-UP HISTORY:`);
        console.log(`   ` + '-'.repeat(120));
        console.log(`   ${'#'.padEnd(3)} ${'Date & Time (UTC)'.padEnd(20)} ${'From Address'.padEnd(45)} ${'Amount (BTC)'.padEnd(20)} ${'Amount (Sats)'.padEnd(15)} ${'Block'.padEnd(10)}`);
        console.log(`   ` + '-'.repeat(120));

        sortedTopUps.forEach((topUp, index) => {
            const dateStr = topUp.date.substring(0, 19);
            const fromAddr = topUp.from.substring(0, 42);
            const btcAmount = this.formatBTC(topUp.value);
            const satsAmount = topUp.valueSats.toLocaleString();
            
            console.log(`   ${(index + 1).toString().padEnd(3)} ${dateStr.padEnd(20)} ${fromAddr.padEnd(45)} ${btcAmount.padEnd(20)} ${satsAmount.padEnd(15)} ${topUp.blockNumber.toString().padEnd(10)}`);
        });
        
        console.log(`   ` + '-'.repeat(120));
    }

    // Updated function - no sample transactions display
    logUncategorizedDetails(uncategorized) {
        if (uncategorized.totalTx === 0) {
            return; // Don't show anything if no uncategorized transactions
        }

        console.log(`\n# UNCATEGORIZED INCOMING TRANSACTIONS:`);
        console.log(`   Total Transactions: ${uncategorized.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(uncategorized.totalAmount)} BTC (${this.formatSats(uncategorized.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${uncategorized.wallets.size.toLocaleString()}`);

        // Value distribution only
        if (uncategorized.valueRanges.size > 0) {
            console.log(`\n   ## VALUE DISTRIBUTION:`);
            const sortedRanges = Array.from(uncategorized.valueRanges.entries())
                .sort((a, b) => b[1].count - a[1].count);
            
            sortedRanges.forEach(([range, data]) => {
                console.log(`   ## ${range}: ${data.count.toLocaleString()} txs (${this.formatSats(data.totalValue).toLocaleString()} sats)`);
            });
        }
    }

    // Updated function to log daily breakdown for last 14 days (excluding today)
    logLast14DaysBreakdown() {
        if (this.last14DaysBreakdown.size === 0) {
			console.log(`   ` + '='.repeat(100));
            console.log(`\n##№ DAILY BREAKDOWN - LAST 14 DAYS:`);
            console.log(`   No data available for the last 14 complete days.`);
            return;
        }

        console.log(`\n## DAILY BREAKDOWN - LAST 14 DAYS:`);
        console.log(`   Current time: ${new Date().toISOString()}`);
        console.log(`   Excluding today as incomplete day`);
        console.log(`   ` + '='.repeat(55));

        // Sort dates in ascending order (oldest first) and exclude today
        const today = new Date().toISOString().split('T')[0];
        const sortedDays = Array.from(this.last14DaysBreakdown.entries())
            .filter(([date, stats]) => date !== today) // Exclude today
            .sort((a, b) => a[0].localeCompare(b[0]));

        if (sortedDays.length === 0) {
            console.log(`   No complete days found for breakdown.`);
            return;
        }

        sortedDays.forEach(([date, stats]) => {
            // Only count incoming wallets (wallets that send TO the game)
            const totalUniqueIncomingWallets = stats.allIncoming.wallets.size;

            const weekday = this.getWeekdayName(date);
            
            // Get TX_COUNT data for this day and calculate share
            const totalTxForDay = this.getTxCountForDay(date);
            const sharePercentage = this.calculateSharePercentage(stats.totalTransactions, totalTxForDay);

            console.log(`\n   ${weekday} ${date}:`); // REMOVED: (Complete Day)
            console.log(`     Total Transactions: ${stats.totalTransactions.toLocaleString()}`);
            if (totalTxForDay > 0) {
                console.log(`          Share: ${sharePercentage}%`);
            }
            console.log(`     Total Unique Wallets: ${totalUniqueIncomingWallets.toLocaleString()}`);
            console.log(`     Sat Wheel Total: ${stats.satWheel.totalTx.toLocaleString()}`);
            
            // Show Users/Agent breakdown if agent is configured and there are 1 sat transactions
            if (this.agentAddress && stats.satWheel.oneSatTx > 0) {
                const userTx = stats.satWheel.oneSatUserTx + stats.satWheel.tenSatsTx;
                const agentTx = stats.satWheel.oneSatAgentTx;
                console.log(`       Users: ${userTx.toLocaleString()}`);
                console.log(`       Agent: ${agentTx.toLocaleString()}`);
            } else if (stats.satWheel.totalTx > 0) {
                // If no agent configured, just show total as users
                console.log(`       Users: ${stats.satWheel.totalTx.toLocaleString()}`);
                console.log(`       Agent: 0`);
            }
            
            console.log(`     Guess the Block: ${stats.guessTheBlock.totalTx.toLocaleString()}`);
            
            // Additional useful daily stats
            if (stats.payouts.totalTx > 0) {
                console.log(`     Payouts: ${stats.payouts.totalTx.toLocaleString()}`);
            }
        });

        // Calculate averages for different periods with correct period mapping
        const averages7Days = this.calculatePeriodAverages(this.analytics.last14Days, 7);
        const averages14Days = this.calculatePeriodAverages(this.analytics.last14Days, 14);
        const averages21Days = this.calculatePeriodAverages(this.analytics.last30Days, 21);
        const averages28Days = this.calculatePeriodAverages(this.analytics.last30Days, 28);

        console.log(`\n   ${'='.repeat(55)}`);

        if (averages7Days && averages14Days && averages21Days && averages28Days) {
            // Header
            console.log(`   ${'Average per day'.padEnd(20)} ${'7 Days'.padEnd(10)} ${'14 Days'.padEnd(10)} ${'21 Days'.padEnd(10)} ${'28 Days'.padEnd(10)}`);
            console.log(`   ${'-'.repeat(20)} ${'-'.repeat(10)} ${'-'.repeat(10)} ${'-'.repeat(10)} ${'-'.repeat(10)}`);
            
            // Transactions
            console.log(`   ${'Transactions:'.padEnd(20)} ${averages7Days.avgTransactions.toLocaleString().padEnd(10)} ${averages14Days.avgTransactions.toLocaleString().padEnd(10)} ${averages21Days.avgTransactions.toLocaleString().padEnd(10)} ${averages28Days.avgTransactions.toLocaleString().padEnd(10)}`);
            
            // Share percentages
            if (this.txCountData.size > 0) {
                console.log(`   ${'      Share:'.padEnd(20)} ${(averages7Days.sharePercentage + '%').padEnd(10)} ${(averages14Days.sharePercentage + '%').padEnd(10)} ${(averages21Days.sharePercentage + '%').padEnd(10)} ${(averages28Days.sharePercentage + '%').padEnd(10)}`);
            }
            
            // Unique Wallets
            console.log(`   ${'Unique Wallets:'.padEnd(20)} ${averages7Days.avgUniqueWallets.toLocaleString().padEnd(10)} ${averages14Days.avgUniqueWallets.toLocaleString().padEnd(10)} ${averages21Days.avgUniqueWallets.toLocaleString().padEnd(10)} ${averages28Days.avgUniqueWallets.toLocaleString().padEnd(10)}`);
            
            // Wheel Total
            console.log(`   ${'Wheel Total:'.padEnd(20)} ${averages7Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages14Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages21Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages28Days.avgSatWheelTx.toLocaleString().padEnd(10)}`);
            
            // Users
            if (this.agentAddress) {
                console.log(`   ${'  Users:'.padEnd(20)} ${averages7Days.avgUserTx.toLocaleString().padEnd(10)} ${averages14Days.avgUserTx.toLocaleString().padEnd(10)} ${averages21Days.avgUserTx.toLocaleString().padEnd(10)} ${averages28Days.avgUserTx.toLocaleString().padEnd(10)}`);
                console.log(`   ${'  Agent:'.padEnd(20)} ${averages7Days.avgAgentTx.toLocaleString().padEnd(10)} ${averages14Days.avgAgentTx.toLocaleString().padEnd(10)} ${averages21Days.avgAgentTx.toLocaleString().padEnd(10)} ${averages28Days.avgAgentTx.toLocaleString().padEnd(10)}`);
            } else {
                console.log(`   ${'  Users:'.padEnd(20)} ${averages7Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages14Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages21Days.avgSatWheelTx.toLocaleString().padEnd(10)} ${averages28Days.avgSatWheelTx.toLocaleString().padEnd(10)}`);
                console.log(`   ${'  Agent:'.padEnd(20)} ${'0'.padEnd(10)} ${'0'.padEnd(10)} ${'0'.padEnd(10)} ${'0'.padEnd(10)}`);
            }
            
            // Guess the Block
            console.log(`   ${'Guess the Block:'.padEnd(20)} ${averages7Days.avgGuessTheBlock.toLocaleString().padEnd(10)} ${averages14Days.avgGuessTheBlock.toLocaleString().padEnd(10)} ${averages21Days.avgGuessTheBlock.toLocaleString().padEnd(10)} ${averages28Days.avgGuessTheBlock.toLocaleString().padEnd(10)}`);
        }

        console.log(`   ` + '='.repeat(100));
    }

     // Updated logPeriodAnalysis - move daily breakdown trigger
    logPeriodAnalysis(periodName, stats) {
        console.log(`\n### ${periodName}`);
        console.log('='.repeat(60));

        // Calculate TX_COUNT data for the period and share percentage
        let totalTxCountForPeriod = 0;
        let sharePercentage = '0.00';
        
        if (this.txCountData.size > 0 && stats.earliestTimestamp !== Infinity && stats.latestTimestamp !== 0) {
            totalTxCountForPeriod = this.getTotalTxCountForPeriod(stats.earliestTimestamp, stats.latestTimestamp);
            sharePercentage = this.calculateSharePercentage(stats.totalTransactions, totalTxCountForPeriod);
        }

        // Enhanced OVERVIEW with transaction counts and values
        console.log(`## OVERVIEW:`);
        console.log(`   Total Transactions: ${stats.totalTransactions.toLocaleString()}`);
        if (totalTxCountForPeriod > 0) {
            console.log(`        Share: ${sharePercentage}%`);
        }
        console.log(`   Block Range: ${stats.blockRange.min === Infinity ? 'N/A' : stats.blockRange.min.toLocaleString()} - ${stats.blockRange.max === 0 ? 'N/A' : stats.blockRange.max.toLocaleString()}`);
        console.log(`   Time Range: ${this.formatDate(stats.earliestTimestamp)} - ${this.formatDate(stats.latestTimestamp)}`);
        
        // Total incoming and outgoing summary
        console.log(`\n   # TOTAL INCOMING:`);
        console.log(`   • Transactions: ${stats.allIncoming.totalTx.toLocaleString()}`);
        console.log(`   • Value: ${this.formatBTC(stats.allIncoming.totalAmount)} BTC (${this.formatSats(stats.allIncoming.totalAmount).toLocaleString()} sats)`);
        console.log(`   • Unique Wallets: ${stats.allIncoming.wallets.size.toLocaleString()}`);
        
        console.log(`\n   # TOTAL OUTGOING:`);
        console.log(`   • Transactions: ${stats.allOutgoing.totalTx.toLocaleString()}`);
        console.log(`   • Value: ${this.formatBTC(stats.allOutgoing.totalAmount)} BTC (${this.formatSats(stats.allOutgoing.totalAmount).toLocaleString()} sats)`);
        console.log(`   • Unique Wallets: ${stats.payouts.wallets.size.toLocaleString()}`);
        
        // Verification check
        const categorizedTotal = stats.satWheel.totalAmount + stats.guessTheBlock.totalAmount + 
                                stats.topUps.totalAmount + stats.uncategorizedIncoming.totalAmount;
        const difference = Math.abs(stats.allIncoming.totalAmount - categorizedTotal);
        
        console.log(`\n## CATEGORIZATION VERIFICATION:`);
        console.log(`   Total Incoming: ${this.formatBTC(stats.allIncoming.totalAmount)} BTC (${this.formatSats(stats.allIncoming.totalAmount).toLocaleString()} sats)`);
        console.log(`   Categorized Total: ${this.formatBTC(categorizedTotal)} BTC (${this.formatSats(categorizedTotal).toLocaleString()} sats)`);
        console.log(`   Difference: ${this.formatBTC(difference)} BTC (${this.formatSats(difference).toLocaleString()} sats) ${difference < 0.00000001 ? '[OK]' : '[ERROR]'}`);

        // Individual categories
        const satWheelAvg = this.calculateWalletAverages(stats.satWheel.wallets, stats.satWheel.totalTx, stats.satWheel.totalAmount);
        console.log(`\n## SAT WHEEL (1 sat & 10 sats):`);
        console.log(`   Total Transactions: ${stats.satWheel.totalTx.toLocaleString()}`);
        
        // Show agent breakdown only if agent address is configured and for 1 sat transactions
        if (this.agentAddress && stats.satWheel.oneSatTx > 0) {
            console.log(`   Total User Transactions: ${(stats.satWheel.oneSatUserTx + stats.satWheel.tenSatsTx).toLocaleString()}`);
            console.log(`   Total Agent Transactions: ${stats.satWheel.oneSatAgentTx.toLocaleString()}`);
        }
        
        console.log(`   # 1 Sat Transactions: ${stats.satWheel.oneSatTx.toLocaleString()}`);
        
        if (this.agentAddress && stats.satWheel.oneSatTx > 0) {
            console.log(`     1 sat user transactions: ${stats.satWheel.oneSatUserTx.toLocaleString()}`);
            console.log(`     1 sat agent transactions: ${stats.satWheel.oneSatAgentTx.toLocaleString()}`);
        }
        
        console.log(`   # 10 Sats Transactions: ${stats.satWheel.tenSatsTx.toLocaleString()}`);
        
        console.log(`   Total Value: ${this.formatBTC(stats.satWheel.totalAmount)} BTC (${this.formatSats(stats.satWheel.totalAmount).toLocaleString()} sats)`);
        
        if (this.agentAddress && (stats.satWheel.userAmount > 0 || stats.satWheel.agentAmount > 0)) {
            console.log(`   User Value: ${this.formatBTC(stats.satWheel.userAmount)} BTC (${this.formatSats(stats.satWheel.userAmount).toLocaleString()} sats)`);
            console.log(`   Agent Value: ${this.formatBTC(stats.satWheel.agentAmount)} BTC (${this.formatSats(stats.satWheel.agentAmount).toLocaleString()} sats)`);
        }
        
        console.log(`   Unique Wallets: ${satWheelAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${satWheelAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(satWheelAvg.avgValuePerWallet)} BTC (${Math.round(satWheelAvg.avgSatsPerWallet).toLocaleString()} sats)`);

        const gtbAvg = this.calculateWalletAverages(stats.guessTheBlock.wallets, stats.guessTheBlock.totalTx, stats.guessTheBlock.totalAmount);
        console.log(`\n## GUESS THE BLOCK (100 sats):`);
        console.log(`   Total Transactions: ${stats.guessTheBlock.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(stats.guessTheBlock.totalAmount)} BTC (${this.formatSats(stats.guessTheBlock.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${gtbAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${gtbAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(gtbAvg.avgValuePerWallet)} BTC (${Math.round(gtbAvg.avgSatsPerWallet).toLocaleString()} sats)`);

        if (periodName === 'ALL TIME') {
            console.log(`\n## TOP UPS (>100 sats):`);
            console.log(`   Total Operations: ${stats.topUps.totalOps.toLocaleString()}`);
            console.log(`   Total Amount: ${this.formatBTC(stats.topUps.totalAmount)} BTC (${this.formatSats(stats.topUps.totalAmount).toLocaleString()} sats)`);
            console.log(`   Largest Top Up: ${this.formatBTC(stats.topUps.largestTopUp)} BTC (${this.formatSats(stats.topUps.largestTopUp).toLocaleString()} sats)`);
            console.log(`   Smallest Top Up: ${this.formatBTC(stats.topUps.smallestTopUp)} BTC (${this.formatSats(stats.topUps.smallestTopUp).toLocaleString()} sats)`);
            this.logTopUpDetails(stats.topUps);
        }

        // Show uncategorized transactions (summary only, no samples)
        this.logUncategorizedDetails(stats.uncategorizedIncoming);

        const payoutAvg = this.calculateWalletAverages(stats.payouts.wallets, stats.payouts.totalTx, stats.payouts.totalAmount);
        console.log(`\n## PAYOUTS:`);
        console.log(`   Total Transactions: ${stats.payouts.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(stats.payouts.totalAmount)} BTC (${this.formatSats(stats.payouts.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${payoutAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${payoutAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(payoutAvg.avgValuePerWallet)} BTC (${Math.round(payoutAvg.avgSatsPerWallet).toLocaleString()} sats)`);
        console.log(`   Largest Payout: ${this.formatBTC(stats.payouts.largestPayout)} BTC (${this.formatSats(stats.payouts.largestPayout).toLocaleString()} sats)`);
        console.log(`   Smallest Payout: ${this.formatBTC(stats.payouts.smallestPayout)} BTC (${this.formatSats(stats.payouts.smallestPayout).toLocaleString()} sats)`);
		const avgPayoutValue = stats.payouts.totalTx > 0 ? stats.payouts.totalAmount / stats.payouts.totalTx : 0;
console.log(`   Avg payout value: ${this.formatBTC(avgPayoutValue)} BTC (${Math.round(avgPayoutValue * 100000000).toLocaleString()} sats)`);

        // Ratios
        const gamingIn = stats.gamingIncoming.totalAmount;
        const totalOut = stats.allOutgoing.totalAmount;
        const ratio = totalOut > 0 ? gamingIn / totalOut : gamingIn > 0 ? Infinity : 0;

        console.log(`\n## GAMING IN/OUT RATIO (excludes top-ups & uncategorized):`);
        console.log(`   Gaming Income: ${this.formatBTC(gamingIn)} BTC (${this.formatSats(gamingIn).toLocaleString()} sats)`);
        console.log(`   Total Payouts: ${this.formatBTC(totalOut)} BTC (${this.formatSats(totalOut).toLocaleString()} sats)`);
        console.log(`   Gaming Ratio (In/Out): ${ratio === Infinity ? 'INFINITY' : ratio.toFixed(4)}`);
        
        const fullIn = stats.allIncoming.totalAmount;
        const fullRatio = totalOut > 0 ? fullIn / totalOut : fullIn > 0 ? Infinity : 0;
        console.log(`\n## FULL IN/OUT RATIO (includes everything):`);
        console.log(`   Total Income: ${this.formatBTC(fullIn)} BTC (${this.formatSats(fullIn).toLocaleString()} sats)`);
        console.log(`   Total Payouts: ${this.formatBTC(totalOut)} BTC (${this.formatSats(totalOut).toLocaleString()} sats)`);
        console.log(`   Full Ratio (In/Out): ${fullRatio === Infinity ? 'INFINITY' : fullRatio.toFixed(4)}`);

        // REMOVED: Daily breakdown trigger from here - it will be called after all periods

        // Top Wallets Summary (only for all time to avoid clutter)
        if (periodName === 'ALL TIME' && stats.satWheel.wallets.size > 0) {
            const topSatWheelWallets = this.getTopWallets(stats.satWheel.wallets, 5);
            console.log(`\n## TOP 5 SAT WHEEL PLAYERS:`);
            topSatWheelWallets.forEach((wallet, index) => {
                console.log(`   ${index + 1}. ${wallet.address.substring(0, 10)}... - ${wallet.count} txs, ${this.formatSats(wallet.totalValue)} sats`);
            });
        }

        if (periodName === 'ALL TIME' && stats.payouts.wallets.size > 0) {
            const topPayoutWallets = this.getTopWallets(stats.payouts.wallets, 5);
            console.log(`\n## TOP 5 PAYOUT RECIPIENTS:`);
            topPayoutWallets.forEach((wallet, index) => {
                console.log(`   ${index + 1}. ${wallet.address.substring(0, 10)}... - ${wallet.count} txs, ${this.formatBTC(wallet.totalValue)} BTC`);
            });
        }
    }

    getTopWallets(walletMap, limit = 10) {
        const wallets = Array.from(walletMap.entries())
            .map(([address, data]) => ({ address, ...data }))
            .sort((a, b) => b.totalValue - a.totalValue)
            .slice(0, limit);
        
        return wallets;
    }

        generateReport() {
        console.log(`\n# COMPREHENSIVE TRANSACTION ANALYSIS WITH COMPLETE CATEGORIZATION`);
        console.log(`# Analysis Date: ${new Date().toISOString()}`);
        console.log(`# Target Address: ${this.address}`);
        console.log(`# CSV File: ${this.csvFilename}`);
        console.log(`# Total Processed: ${this.processedCount.toLocaleString()} transactions`);
        console.log(`# NOTE: Excluding incomplete days (today: ${new Date().toISOString().split('T')[0]}) - COMPLETE DAYS ONLY`);
        if (this.agentAddress) {
            console.log(`# Agent Address: ${this.agentAddress}`);
        }
        
        // Include duplicate information in report
        if (this.duplicateCount > 0) {
            console.log(`# Duplicate Transactions Found: ${this.duplicateTransactions.size.toLocaleString()} unique TXIDs with ${this.duplicateCount.toLocaleString()} total duplicates`);
        } else {
            console.log(`# Duplicate Check: ✓ No duplicates found`);
        }
        
        if (this.txCountData.size > 0) {
            console.log(`# TX_COUNT Data: Available for ${this.txCountData.size.toLocaleString()} days`);
        }
        
        try {
            const stats = fs.statSync(this.csvFilename);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
            console.log(`# File Size: ${fileSizeInMB} MB`);
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] WARN: # Could not get file size`);
        }

        // CORRECT ORDER: All periods first, then daily breakdown at the very end
        this.logPeriodAnalysis('ALL TIME', this.analytics.allTime);
        this.logPeriodAnalysis('LAST 30 DAYS', this.analytics.last30Days);
        this.logPeriodAnalysis('LAST 14 DAYS', this.analytics.last14Days);
        this.logPeriodAnalysis('LAST 7 DAYS', this.analytics.last7Days);

        // Daily breakdown appears AFTER all period analyses
        this.logLast14DaysBreakdown();

        console.log(`\n# COMPREHENSIVE ANALYSIS COMPLETE - ALL TRANSACTIONS CATEGORIZED (COMPLETE DAYS ONLY)`);
        console.log('='.repeat(80));
    }

    // Generate CSV data for calendar analysis
    generateCalendarCSV() {
        const csvLines = [];
        
        // CSV Header
        const headers = [
            'Date',
            'Total_Transactions',
            'Total_Incoming_Tx',
            'Total_Incoming_BTC',
            'Total_Incoming_Sats',
            'Total_Outgoing_Tx',
            'Total_Outgoing_BTC',
            'Total_Outgoing_Sats',
            'SatWheel_Total_Tx',
            'SatWheel_1Sat_Tx',
            'SatWheel_10Sats_Tx',
            'SatWheel_User_Tx',
            'SatWheel_Agent_Tx',
            'SatWheel_Total_BTC',
            'SatWheel_Total_Sats',
            'SatWheel_User_BTC',
            'SatWheel_User_Sats',
            'SatWheel_Agent_BTC',
            'SatWheel_Agent_Sats',
            'GuessTheBlock_Tx',
            'GuessTheBlock_BTC',
            'GuessTheBlock_Sats',
            'TopUps_Tx',
            'TopUps_BTC',
            'TopUps_Sats',
            'Uncategorized_Tx',
            'Uncategorized_BTC',
            'Uncategorized_Sats',
            'Payouts_Tx',
            'Payouts_BTC',
            'Payouts_Sats',
            'Gaming_Income_BTC',
            'Gaming_Income_Sats',
            'Gaming_Ratio',
            'Full_Ratio',
            'Unique_Incoming_Wallets',
            'Unique_Outgoing_Wallets',
            'Block_Range_Min',
            'Block_Range_Max',
            'Total_Network_Tx',
            'Share_Percentage'
        ];
        
        csvLines.push(headers.join(','));
        
        // Sort daily data by date and exclude today
        const today = new Date().toISOString().split('T')[0];
        const sortedDays = Array.from(this.dailyAnalytics.entries())
            .filter(([date, stats]) => date !== today) // Exclude today
            .sort((a, b) => a[0].localeCompare(b[0]));
        
        for (const [date, stats] of sortedDays) {
            const gamingIn = stats.gamingIncoming.totalAmount;
            const totalOut = stats.allOutgoing.totalAmount;
            const gamingRatio = totalOut > 0 ? gamingIn / totalOut : (gamingIn > 0 ? 'INFINITY' : 0);
            const fullIn = stats.allIncoming.totalAmount;
            const fullRatio = totalOut > 0 ? fullIn / totalOut : (fullIn > 0 ? 'INFINITY' : 0);
            
            // Get TX_COUNT data for this day and calculate share
            const totalTxForDay = this.getTxCountForDay(date);
            const sharePercentage = this.calculateSharePercentage(stats.totalTransactions, totalTxForDay);
            
            const row = [
                date,
                stats.totalTransactions,
                stats.allIncoming.totalTx,
                this.formatBTC(stats.allIncoming.totalAmount),
                this.formatSats(stats.allIncoming.totalAmount),
                stats.allOutgoing.totalTx,
                this.formatBTC(stats.allOutgoing.totalAmount),
                this.formatSats(stats.allOutgoing.totalAmount),
                stats.satWheel.totalTx,
                stats.satWheel.oneSatTx,
                stats.satWheel.tenSatsTx,
                stats.satWheel.oneSatUserTx + stats.satWheel.tenSatsTx,
                stats.satWheel.oneSatAgentTx,
                this.formatBTC(stats.satWheel.totalAmount),
                this.formatSats(stats.satWheel.totalAmount),
                this.formatBTC(stats.satWheel.userAmount),
                this.formatSats(stats.satWheel.userAmount),
                this.formatBTC(stats.satWheel.agentAmount),
                this.formatSats(stats.satWheel.agentAmount),
                stats.guessTheBlock.totalTx,
                this.formatBTC(stats.guessTheBlock.totalAmount),
                this.formatSats(stats.guessTheBlock.totalAmount),
                stats.topUps.totalOps,
                this.formatBTC(stats.topUps.totalAmount),
                this.formatSats(stats.topUps.totalAmount),
                stats.uncategorizedIncoming.totalTx,
                this.formatBTC(stats.uncategorizedIncoming.totalAmount),
                this.formatSats(stats.uncategorizedIncoming.totalAmount),
                stats.payouts.totalTx,
                this.formatBTC(stats.payouts.totalAmount),
                this.formatSats(stats.payouts.totalAmount),
                this.formatBTC(gamingIn),
                this.formatSats(gamingIn),
                gamingRatio === 'INFINITY' ? 'INFINITY' : (typeof gamingRatio === 'number' ? gamingRatio.toFixed(4) : gamingRatio),
                fullRatio === 'INFINITY' ? 'INFINITY' : (typeof fullRatio === 'number' ? fullRatio.toFixed(4) : fullRatio),
                stats.allIncoming.wallets.size,
                stats.payouts.wallets.size,
                stats.blockRange.min === Infinity ? 'N/A' : stats.blockRange.min,
                stats.blockRange.max === 0 ? 'N/A' : stats.blockRange.max,
                totalTxForDay,
                sharePercentage
            ];
            
            csvLines.push(row.join(','));
        }
        
        return csvLines.join('\n');
    }

    // Export data to JSON for further analysis
    async exportToJSON(filename = null) {
        if (!filename) {
            filename = `${this.address}_analysis_${Date.now()}.json`;
        }

        const exportData = {
            address: this.address,
            agentAddress: this.agentAddress,
            csvFile: this.csvFilename,
            analysisDate: new Date().toISOString(),
            totalProcessed: this.processedCount,
            excludeIncompleteDay: true,
            currentDateExcluded: new Date().toISOString().split('T')[0],
            duplicateInfo: {
                duplicateTransactions: this.duplicateTransactions.size,
                totalDuplicates: this.duplicateCount
            },
            txCountData: Object.fromEntries(this.txCountData),
            thresholds: this.THRESHOLDS,
            analytics: {
                allTime: this.serializeStats(this.analytics.allTime),
                last30Days: this.serializeStats(this.analytics.last30Days),
                last14Days: this.serializeStats(this.analytics.last14Days),
                last7Days: this.serializeStats(this.analytics.last7Days)
            }
        };

        try {
            await fs.promises.writeFile(filename, JSON.stringify(exportData, null, 2));
            console.log(`[${new Date().toISOString()}] INFO: # Analysis exported to: ${filename}`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: # Failed to export analysis:`, error);
        }
    }

    // Export data to TXT with filename based on last block and date
    async exportToTXT() {
        const stats = this.analytics.allTime;
        const lastBlock = stats.blockRange.max;
        const lastTimestamp = stats.latestTimestamp;
        
        // Format date as YYYY-MM-DD_HH-MM-SS
        const date = new Date(lastTimestamp * 1000);
        const formattedDate = date.toISOString()
            .replace(/T/, '_')
            .replace(/:/g, '-')
            .replace(/\..+/, '');
        
        const filename = `${lastBlock}_${formattedDate}_complete_days.txt`;
        const textReport = this.generateTextReport();

        try {
            await fs.promises.writeFile(filename, textReport);
            console.log(`[${new Date().toISOString()}] INFO: ## Text report exported to: ${filename}`);
            return filename;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ## Failed to export text report:`, error);
            return null;
        }
    }

    // Export daily calendar data to CSV
    async exportCalendarCSV() {
        const stats = this.analytics.allTime;
        const lastBlock = stats.blockRange.max;
        const lastTimestamp = stats.latestTimestamp;
        
        // Format date as YYYY-MM-DD_HH-MM-SS
        const date = new Date(lastTimestamp * 1000);
        const formattedDate = date.toISOString()
            .replace(/T/, '_')
            .replace(/:/g, '-')
            .replace(/\..+/, '');
        
        const filename = `${lastBlock}_${formattedDate}_calendar_complete_days.csv`;
        const csvData = this.generateCalendarCSV();

        try {
            await fs.promises.writeFile(filename, csvData);
            console.log(`[${new Date().toISOString()}] INFO: ## Calendar CSV exported to: ${filename}`);
            return filename;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ## Failed to export calendar CSV:`, error);
            return null;
        }
    }

    // Generate text report for export (simplified version of the console output)
    generateTextReport() {
        const lines = [];
        
        lines.push('COMPREHENSIVE TRANSACTION ANALYSIS WITH COMPLETE CATEGORIZATION');
        lines.push('='.repeat(80));
        lines.push(`Analysis Date: ${new Date().toISOString()}`);
        lines.push(`Target Address: ${this.address}`);
        lines.push(`CSV File: ${this.csvFilename}`);
        lines.push(`Total Processed: ${this.processedCount.toLocaleString()} transactions`);
        lines.push(`NOTE: Excluding incomplete days (today: ${new Date().toISOString().split('T')[0]}) - COMPLETE DAYS ONLY`);
        if (this.agentAddress) {
            lines.push(`Agent Address: ${this.agentAddress}`);
        }
        
        if (this.duplicateCount > 0) {
            lines.push(`Duplicate Transactions Found: ${this.duplicateTransactions.size.toLocaleString()} unique TXIDs with ${this.duplicateCount.toLocaleString()} total duplicates`);
        } else {
            lines.push(`Duplicate Check: No duplicates found`);
        }
        
        if (this.txCountData.size > 0) {
            lines.push(`TX_COUNT Data: Available for ${this.txCountData.size.toLocaleString()} days`);
        }
        
        try {
            const stats = fs.statSync(this.csvFilename);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
            lines.push(`File Size: ${fileSizeInMB} MB`);
        } catch (error) {
            lines.push(`File Size: N/A`);
        }

        // Add period analysis summaries
        const periods = [
            { name: 'ALL TIME', key: 'allTime' },
            { name: 'LAST 30 DAYS', key: 'last30Days' },
            { name: 'LAST 14 DAYS', key: 'last14Days' },
            { name: 'LAST 7 DAYS', key: 'last7Days' }
        ];

        periods.forEach(({ name: periodName, key: periodKey }) => {
            const stats = this.analytics[periodKey];
            
            lines.push('');
            lines.push(`### ${periodName}`);
            lines.push('='.repeat(60));
            
            lines.push(`## OVERVIEW:`);
            lines.push(`   Total Transactions: ${stats.totalTransactions.toLocaleString()}`);
            lines.push(`   Block Range: ${stats.blockRange.min === Infinity ? 'N/A' : stats.blockRange.min.toLocaleString()} - ${stats.blockRange.max === 0 ? 'N/A' : stats.blockRange.max.toLocaleString()}`);
            lines.push(`   Time Range: ${this.formatDate(stats.earliestTimestamp)} - ${this.formatDate(stats.latestTimestamp)}`);
            
            lines.push(`\n   TOTAL INCOMING: ${stats.allIncoming.totalTx.toLocaleString()} tx, ${this.formatSats(stats.allIncoming.totalAmount).toLocaleString()} sats`);
            lines.push(`   TOTAL OUTGOING: ${stats.allOutgoing.totalTx.toLocaleString()} tx, ${this.formatSats(stats.allOutgoing.totalAmount).toLocaleString()} sats`);
            lines.push(`   SAT WHEEL: ${stats.satWheel.totalTx.toLocaleString()} tx, ${this.formatSats(stats.satWheel.totalAmount).toLocaleString()} sats`);
            lines.push(`   GUESS THE BLOCK: ${stats.guessTheBlock.totalTx.toLocaleString()} tx, ${this.formatSats(stats.guessTheBlock.totalAmount).toLocaleString()} sats`);
            lines.push(`   PAYOUTS: ${stats.payouts.totalTx.toLocaleString()} tx, ${this.formatSats(stats.payouts.totalAmount).toLocaleString()} sats`);
        });

        lines.push('');
        lines.push('COMPREHENSIVE ANALYSIS COMPLETE - ALL TRANSACTIONS CATEGORIZED (COMPLETE DAYS ONLY)');
        lines.push('='.repeat(80));
        
        return lines.join('\n');
    }

    serializeStats(stats) {
        // Convert Maps to Objects for JSON serialization
        return {
            ...stats,
            satWheel: {
                ...stats.satWheel,
                wallets: Object.fromEntries(stats.satWheel.wallets)
            },
            guessTheBlock: {
                ...stats.guessTheBlock,
                wallets: Object.fromEntries(stats.guessTheBlock.wallets)
            },
            allIncoming: {
                ...stats.allIncoming,
                wallets: Object.fromEntries(stats.allIncoming.wallets)
            },
            gamingIncoming: {
                ...stats.gamingIncoming,
                wallets: Object.fromEntries(stats.gamingIncoming.wallets)
            },
            uncategorizedIncoming: {
                ...stats.uncategorizedIncoming,
                wallets: Object.fromEntries(stats.uncategorizedIncoming.wallets),
                valueRanges: Object.fromEntries(stats.uncategorizedIncoming.valueRanges)
            },
            payouts: {
                ...stats.payouts,
                wallets: Object.fromEntries(stats.payouts.wallets)
            }
        };
    }
}

// Main execution function
async function main() {
    try {
        const csvFilename = process.argv[2] || process.env.CSV_FILENAME;
        
        if (!csvFilename) {
            console.error(`[${new Date().toISOString()}] ERROR: ## Please provide CSV filename as argument or set CSV_FILENAME environment variable`);
            console.error(`[${new Date().toISOString()}] INFO: ## Usage: node analyze.js <csv_filename>`);
            console.error(`[${new Date().toISOString()}] INFO: ## Example: node analyze.js 0xfb8e879cb77aeb594850da75f30c7d777ce54513.csv`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set AGENT environment variable to track agent transactions separately for 1 sat only`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set TX_COUNT environment variable with transaction count data or API URL`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set DEBUG=1 to enable debug logging, DEBUG=0 to disable (default)`);
            console.error(`[${new Date().toISOString()}] INFO: ## NOTE: Analysis now excludes incomplete days (today) for accurate reporting`);
            process.exit(1);
        }
        
        try {
            fs.accessSync(csvFilename);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ## CSV file not found: ${csvFilename}`);
            process.exit(1);
        }
        
        const analyzer = new ComprehensiveTransactionAnalyzer(csvFilename);
        await analyzer.init(); // Initialize TX_COUNT data
        
        console.log(`[${new Date().toISOString()}] INFO: ## Starting comprehensive analysis with complete categorization (excluding incomplete days)...`);
        const startTime = Date.now();
        
        await analyzer.analyzeStreaming();
        analyzer.generateReport();
        
        // Always export to TXT and CSV
        await analyzer.exportToTXT();
        await analyzer.exportCalendarCSV();
        
        // Optional: Export to JSON
        const shouldExport = process.argv.includes('--export') || process.env.EXPORT_JSON === 'true';
        if (shouldExport) {
            await analyzer.exportToJSON();
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`[${new Date().toISOString()}] INFO: ## Total analysis time: ${duration} seconds`);
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ## Comprehensive analysis failed:`, error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = { ComprehensiveTransactionAnalyzer };