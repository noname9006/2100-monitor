const fs = require('fs');
const readline = require('readline');
const path = require('path');

require('dotenv').config();

class ComprehensiveTransactionAnalyzer {
    constructor(csvFilename) {
        this.csvFilename = csvFilename;
        this.address = this.extractAddressFromFilename(csvFilename);
        this.processedCount = 0;
        
        // Define thresholds in BTC
        this.THRESHOLDS = {
            ONE_SAT: 0.00000001,
            TEN_SATS: 0.0000001,
            HUNDRED_SATS: 0.000001,
            TOP_UP_MIN: 0.000001
        };
        
        // Time cutoffs (in seconds since epoch)
        const now = Math.floor(Date.now() / 1000);
        this.timeCutoffs = {
            last7Days: now - (7 * 24 * 60 * 60),
            last30Days: now - (30 * 24 * 60 * 60)
        };
        
        // Initialize analytics containers
        this.initializeAnalytics();
        
        console.log(`[${new Date().toISOString()}] INFO: 📊 ComprehensiveTransactionAnalyzer initialized for address: ${this.address}`);
        console.log(`[${new Date().toISOString()}] INFO: 📄 CSV file: ${this.csvFilename}`);
    }

    extractAddressFromFilename(filename) {
        const baseName = path.basename(filename, '.csv');
        return baseName.toLowerCase();
    }

    initializeAnalytics() {
        this.analytics = {
            allTime: this.createEmptyStats(),
            last30Days: this.createEmptyStats(),
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
                tenSatsTx: 0
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

    processTransaction(tx) {
        // Determine if transaction is incoming or outgoing
        const isIncoming = tx.to === this.address;
        const isOutgoing = tx.from === this.address;
        
        if (!isIncoming && !isOutgoing) return;

        // Filter out zero-value transactions
        if (tx.value <= 0 || tx.value < 1e-18) return;

        // Process for all time periods
        const periods = ['allTime'];
        if (tx.timestamp >= this.timeCutoffs.last30Days) periods.push('last30Days');
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

        // Categorize EVERY transaction precisely
        if (this.isFloatEqual(tx.value, this.THRESHOLDS.ONE_SAT)) {
            // Exactly 1 Sat
            stats.satWheel.totalTx++;
            stats.satWheel.totalAmount += tx.value;
            stats.satWheel.oneSatTx++;
            this.updateWalletMap(stats.satWheel.wallets, tx.from, tx.value);
            
            stats.gamingIncoming.totalTx++;
            stats.gamingIncoming.totalAmount += tx.value;
            this.updateWalletMap(stats.gamingIncoming.wallets, tx.from, tx.value);
            
        } else if (this.isFloatEqual(tx.value, this.THRESHOLDS.TEN_SATS)) {
            // Exactly 10 Sats
            stats.satWheel.totalTx++;
            stats.satWheel.totalAmount += tx.value;
            stats.satWheel.tenSatsTx++;
            this.updateWalletMap(stats.satWheel.wallets, tx.from, tx.value);
            
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

    async analyzeStreaming() {
        return new Promise((resolve, reject) => {
            try {
                console.log(`[${new Date().toISOString()}] INFO: 🔄 Starting comprehensive streaming analysis...`);
                
                const fileStream = fs.createReadStream(this.csvFilename);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });

                let isFirstLine = true;
                let lineCount = 0;
                let errorCount = 0;

                rl.on('line', (line) => {
                    try {
                        lineCount++;
                        
                        if (isFirstLine) {
                            isFirstLine = false;
                            console.log(`[${new Date().toISOString()}] INFO: 📄 CSV Header: ${line}`);
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
                                this.processTransaction(tx);
                                this.processedCount++;
                            }
                        }

                        if (lineCount % 100000 === 0) {
                            console.log(`[${new Date().toISOString()}] INFO: 📈 Processed ${lineCount.toLocaleString()} lines, ${this.processedCount.toLocaleString()} valid transactions`);
                            
                            if (global.gc) {
                                global.gc();
                            }
                        }

                    } catch (error) {
                        errorCount++;
                        if (errorCount <= 10) {
                            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Error parsing line ${lineCount}: ${error.message}`);
                        }
                    }
                });

                rl.on('close', () => {
                    console.log(`[${new Date().toISOString()}] INFO: ✅ Successfully processed ${this.processedCount.toLocaleString()} valid transactions from ${lineCount.toLocaleString()} lines`);
                    if (errorCount > 0) {
                        console.log(`[${new Date().toISOString()}] WARN: ⚠️ Encountered ${errorCount} parsing errors`);
                    }
                    resolve();
                });

                rl.on('error', (error) => {
                    console.error(`[${new Date().toISOString()}] ERROR: ❌ Error reading file:`, error);
                    reject(error);
                });

            } catch (error) {
                console.error(`[${new Date().toISOString()}] ERROR: ❌ Error setting up file stream:`, error);
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
        return new Date(timestamp * 1000).toISOString().replace('T', ' ').replace('Z', ' UTC');
    }

    logTopUpDetails(topUps) {
        if (topUps.transactions.length === 0) {
            console.log(`   No top-ups found.`);
            return;
        }

        const sortedTopUps = topUps.transactions.sort((a, b) => b.timestamp - a.timestamp);

        console.log(`\n   📋 DETAILED TOP-UP HISTORY:`);
        console.log(`   ` + '─'.repeat(120));
        console.log(`   ${'#'.padEnd(3)} ${'Date & Time (UTC)'.padEnd(20)} ${'From Address'.padEnd(45)} ${'Amount (BTC)'.padEnd(20)} ${'Amount (Sats)'.padEnd(15)} ${'Block'.padEnd(10)}`);
        console.log(`   ` + '─'.repeat(120));

        sortedTopUps.forEach((topUp, index) => {
            const dateStr = topUp.date.substring(0, 19);
            const fromAddr = topUp.from.substring(0, 42);
            const btcAmount = this.formatBTC(topUp.value);
            const satsAmount = topUp.valueSats.toLocaleString();
            
            console.log(`   ${(index + 1).toString().padEnd(3)} ${dateStr.padEnd(20)} ${fromAddr.padEnd(45)} ${btcAmount.padEnd(20)} ${satsAmount.padEnd(15)} ${topUp.blockNumber.toString().padEnd(10)}`);
        });
        
        console.log(`   ` + '─'.repeat(120));
    }

    // Updated function - no sample transactions display
    logUncategorizedDetails(uncategorized) {
        if (uncategorized.totalTx === 0) {
            return; // Don't show anything if no uncategorized transactions
        }

        console.log(`\n🔍 UNCATEGORIZED INCOMING TRANSACTIONS:`);
        console.log(`   Total Transactions: ${uncategorized.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(uncategorized.totalAmount)} BTC (${this.formatSats(uncategorized.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${uncategorized.wallets.size.toLocaleString()}`);

        // Value distribution only
        if (uncategorized.valueRanges.size > 0) {
            console.log(`\n   📊 VALUE DISTRIBUTION:`);
            const sortedRanges = Array.from(uncategorized.valueRanges.entries())
                .sort((a, b) => b[1].count - a[1].count);
            
            sortedRanges.forEach(([range, data]) => {
                console.log(`   • ${range}: ${data.count.toLocaleString()} txs (${this.formatSats(data.totalValue).toLocaleString()} sats)`);
            });
        }
    }

    logPeriodAnalysis(periodName, stats) {
        console.log(`\n📊 ${periodName}`);
        console.log('═'.repeat(60));

        // Enhanced OVERVIEW with transaction counts and values
        console.log(`📈 OVERVIEW:`);
        console.log(`   Total Transactions: ${stats.totalTransactions.toLocaleString()}`);
        console.log(`   Block Range: ${stats.blockRange.min === Infinity ? 'N/A' : stats.blockRange.min.toLocaleString()} - ${stats.blockRange.max === 0 ? 'N/A' : stats.blockRange.max.toLocaleString()}`);
        console.log(`   Time Range: ${this.formatDate(stats.earliestTimestamp)} - ${this.formatDate(stats.latestTimestamp)}`);
        
        // Total incoming and outgoing summary
        console.log(`\n   📥 TOTAL INCOMING:`);
        console.log(`   • Transactions: ${stats.allIncoming.totalTx.toLocaleString()}`);
        console.log(`   • Value: ${this.formatBTC(stats.allIncoming.totalAmount)} BTC (${this.formatSats(stats.allIncoming.totalAmount).toLocaleString()} sats)`);
        console.log(`   • Unique Wallets: ${stats.allIncoming.wallets.size.toLocaleString()}`);
        
        console.log(`\n   📤 TOTAL OUTGOING:`);
        console.log(`   • Transactions: ${stats.allOutgoing.totalTx.toLocaleString()}`);
        console.log(`   • Value: ${this.formatBTC(stats.allOutgoing.totalAmount)} BTC (${this.formatSats(stats.allOutgoing.totalAmount).toLocaleString()} sats)`);
        console.log(`   • Unique Wallets: ${stats.payouts.wallets.size.toLocaleString()}`);
        
        // Verification check
        const categorizedTotal = stats.satWheel.totalAmount + stats.guessTheBlock.totalAmount + 
                                stats.topUps.totalAmount + stats.uncategorizedIncoming.totalAmount;
        const difference = Math.abs(stats.allIncoming.totalAmount - categorizedTotal);
        
        console.log(`\n🔬 CATEGORIZATION VERIFICATION:`);
        console.log(`   Total Incoming: ${this.formatBTC(stats.allIncoming.totalAmount)} BTC (${this.formatSats(stats.allIncoming.totalAmount).toLocaleString()} sats)`);
        console.log(`   Categorized Total: ${this.formatBTC(categorizedTotal)} BTC (${this.formatSats(categorizedTotal).toLocaleString()} sats)`);
        console.log(`   Difference: ${this.formatBTC(difference)} BTC (${this.formatSats(difference).toLocaleString()} sats) ${difference < 0.00000001 ? '✅' : '❌'}`);

        // Individual categories
        const satWheelAvg = this.calculateWalletAverages(stats.satWheel.wallets, stats.satWheel.totalTx, stats.satWheel.totalAmount);
        console.log(`\n🎰 SAT WHEEL (1 sat & 10 sats):`);
        console.log(`   Total Transactions: ${stats.satWheel.totalTx.toLocaleString()}`);
        console.log(`   • 1 Sat Transactions: ${stats.satWheel.oneSatTx.toLocaleString()}`);
        console.log(`   • 10 Sats Transactions: ${stats.satWheel.tenSatsTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(stats.satWheel.totalAmount)} BTC (${this.formatSats(stats.satWheel.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${satWheelAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${satWheelAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(satWheelAvg.avgValuePerWallet)} BTC (${Math.round(satWheelAvg.avgSatsPerWallet).toLocaleString()} sats)`);

        const gtbAvg = this.calculateWalletAverages(stats.guessTheBlock.wallets, stats.guessTheBlock.totalTx, stats.guessTheBlock.totalAmount);
        console.log(`\n🎲 GUESS THE BLOCK (100 sats):`);
        console.log(`   Total Transactions: ${stats.guessTheBlock.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(stats.guessTheBlock.totalAmount)} BTC (${this.formatSats(stats.guessTheBlock.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${gtbAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${gtbAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(gtbAvg.avgValuePerWallet)} BTC (${Math.round(gtbAvg.avgSatsPerWallet).toLocaleString()} sats)`);

        if (periodName === 'ALL TIME') {
            console.log(`\n💰 TOP UPS (>100 sats):`);
            console.log(`   Total Operations: ${stats.topUps.totalOps.toLocaleString()}`);
            console.log(`   Total Amount: ${this.formatBTC(stats.topUps.totalAmount)} BTC (${this.formatSats(stats.topUps.totalAmount).toLocaleString()} sats)`);
            console.log(`   Largest Top Up: ${this.formatBTC(stats.topUps.largestTopUp)} BTC (${this.formatSats(stats.topUps.largestTopUp).toLocaleString()} sats)`);
            console.log(`   Smallest Top Up: ${this.formatBTC(stats.topUps.smallestTopUp)} BTC (${this.formatSats(stats.topUps.smallestTopUp).toLocaleString()} sats)`);
            this.logTopUpDetails(stats.topUps);
        }

        // Show uncategorized transactions (summary only, no samples)
        this.logUncategorizedDetails(stats.uncategorizedIncoming);

        const payoutAvg = this.calculateWalletAverages(stats.payouts.wallets, stats.payouts.totalTx, stats.payouts.totalAmount);
        console.log(`\n💸 PAYOUTS:`);
        console.log(`   Total Transactions: ${stats.payouts.totalTx.toLocaleString()}`);
        console.log(`   Total Value: ${this.formatBTC(stats.payouts.totalAmount)} BTC (${this.formatSats(stats.payouts.totalAmount).toLocaleString()} sats)`);
        console.log(`   Unique Wallets: ${payoutAvg.uniqueWallets.toLocaleString()}`);
        console.log(`   Avg TX per Wallet: ${payoutAvg.avgTxPerWallet.toFixed(2)}`);
        console.log(`   Avg Value per Wallet: ${this.formatBTC(payoutAvg.avgValuePerWallet)} BTC (${Math.round(payoutAvg.avgSatsPerWallet).toLocaleString()} sats)`);
        console.log(`   Largest Payout: ${this.formatBTC(stats.payouts.largestPayout)} BTC (${this.formatSats(stats.payouts.largestPayout).toLocaleString()} sats)`);
        console.log(`   Smallest Payout: ${this.formatBTC(stats.payouts.smallestPayout)} BTC (${this.formatSats(stats.payouts.smallestPayout).toLocaleString()} sats)`);

        // Ratios
        const gamingIn = stats.gamingIncoming.totalAmount;
        const totalOut = stats.allOutgoing.totalAmount;
        const ratio = totalOut > 0 ? gamingIn / totalOut : gamingIn > 0 ? Infinity : 0;

        console.log(`\n⚖️  GAMING IN/OUT RATIO (excludes top-ups & uncategorized):`);
        console.log(`   Gaming Income: ${this.formatBTC(gamingIn)} BTC (${this.formatSats(gamingIn).toLocaleString()} sats)`);
        console.log(`   Total Payouts: ${this.formatBTC(totalOut)} BTC (${this.formatSats(totalOut).toLocaleString()} sats)`);
        console.log(`   Gaming Ratio (In/Out): ${ratio === Infinity ? '∞' : ratio.toFixed(4)}`);
        
        const fullIn = stats.allIncoming.totalAmount;
        const fullRatio = totalOut > 0 ? fullIn / totalOut : fullIn > 0 ? Infinity : 0;
        console.log(`\n📊 FULL IN/OUT RATIO (includes everything):`);
        console.log(`   Total Income: ${this.formatBTC(fullIn)} BTC (${this.formatSats(fullIn).toLocaleString()} sats)`);
        console.log(`   Total Payouts: ${this.formatBTC(totalOut)} BTC (${this.formatSats(totalOut).toLocaleString()} sats)`);
        console.log(`   Full Ratio (In/Out): ${fullRatio === Infinity ? '∞' : fullRatio.toFixed(4)}`);

        // Top Wallets Summary (only for all time to avoid clutter)
        if (periodName === 'ALL TIME' && stats.satWheel.wallets.size > 0) {
            const topSatWheelWallets = this.getTopWallets(stats.satWheel.wallets, 5);
            console.log(`\n🏆 TOP 5 SAT WHEEL PLAYERS:`);
            topSatWheelWallets.forEach((wallet, index) => {
                console.log(`   ${index + 1}. ${wallet.address.substring(0, 10)}... - ${wallet.count} txs, ${this.formatSats(wallet.totalValue)} sats`);
            });
        }

        if (periodName === 'ALL TIME' && stats.payouts.wallets.size > 0) {
            const topPayoutWallets = this.getTopWallets(stats.payouts.wallets, 5);
            console.log(`\n🏆 TOP 5 PAYOUT RECIPIENTS:`);
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
        console.log(`\n🔍 COMPREHENSIVE TRANSACTION ANALYSIS WITH COMPLETE CATEGORIZATION`);
        console.log(`📅 Analysis Date: ${new Date().toISOString()}`);
        console.log(`📊 Target Address: ${this.address}`);
        console.log(`📄 CSV File: ${this.csvFilename}`);
        console.log(`🔢 Total Processed: ${this.processedCount.toLocaleString()} transactions`);
        
        try {
            const stats = fs.statSync(this.csvFilename);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
            console.log(`📏 File Size: ${fileSizeInMB} MB`);
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] WARN: ⚠️ Could not get file size`);
        }

        this.logPeriodAnalysis('ALL TIME', this.analytics.allTime);
        this.logPeriodAnalysis('LAST 30 DAYS', this.analytics.last30Days);
        this.logPeriodAnalysis('LAST 7 DAYS', this.analytics.last7Days);

        console.log(`\n✅ COMPREHENSIVE ANALYSIS COMPLETE - ALL TRANSACTIONS CATEGORIZED`);
        console.log('═'.repeat(80));
    }

    // Export data to JSON for further analysis
    async exportToJSON(filename = null) {
        if (!filename) {
            filename = `${this.address}_analysis_${Date.now()}.json`;
        }

        const exportData = {
            address: this.address,
            csvFile: this.csvFilename,
            analysisDate: new Date().toISOString(),
            totalProcessed: this.processedCount,
            thresholds: this.THRESHOLDS,
            analytics: {
                allTime: this.serializeStats(this.analytics.allTime),
                last30Days: this.serializeStats(this.analytics.last30Days),
                last7Days: this.serializeStats(this.analytics.last7Days)
            }
        };

        try {
            await fs.promises.writeFile(filename, JSON.stringify(exportData, null, 2));
            console.log(`[${new Date().toISOString()}] INFO: 💾 Analysis exported to: ${filename}`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Failed to export analysis:`, error);
        }
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
            console.error(`[${new Date().toISOString()}] ERROR: ❌ Please provide CSV filename as argument or set CSV_FILENAME environment variable`);
            console.error(`[${new Date().toISOString()}] INFO: 💡 Usage: node analyze-comprehensive.js <csv_filename>`);
            console.error(`[${new Date().toISOString()}] INFO: 💡 Example: node analyze-comprehensive.js 0xfb8e879cb77aeb594850da75f30c7d777ce54513.csv`);
            process.exit(1);
        }
        
        try {
            fs.accessSync(csvFilename);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ❌ CSV file not found: ${csvFilename}`);
            process.exit(1);
        }
        
        const analyzer = new ComprehensiveTransactionAnalyzer(csvFilename);
        
        console.log(`[${new Date().toISOString()}] INFO: 🚀 Starting comprehensive analysis with complete categorization...`);
        const startTime = Date.now();
        
        await analyzer.analyzeStreaming();
        analyzer.generateReport();
        
        // Optional: Export to JSON
        const shouldExport = process.argv.includes('--export') || process.env.EXPORT_JSON === 'true';
        if (shouldExport) {
            await analyzer.exportToJSON();
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`[${new Date().toISOString()}] INFO: ⏱️ Total analysis time: ${duration} seconds`);
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ❌ Comprehensive analysis failed:`, error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = { ComprehensiveTransactionAnalyzer };