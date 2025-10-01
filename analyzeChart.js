const fs = require('fs');
const path = require('path');
const { createCanvas } = require('canvas');
const { ComprehensiveTransactionAnalyzer } = require('./analyze.js');

require('dotenv').config();

/**
 * MemoryManager
 * - Monitors process.memoryUsage()
 * - Attempts to GC at thresholds
 * - Stores interval id and exposes cleanup() to stop monitoring
 */
class MemoryManager {
    constructor(maxMemoryMB) {
        this.maxMemoryMB = maxMemoryMB;
        this.maxMemoryBytes = maxMemoryMB * 1024 * 1024;
        this.checkInterval = 1000;

        // Ultra-conservative thresholds for low memory systems
        this.ultraLowMemory = maxMemoryMB < 1024 || process.env.ULTRA_LOW_MEMORY === 'true';

        if (this.ultraLowMemory) {
            this.warningThreshold = 0.25;   // 25%
            this.criticalThreshold = 0.4;   // 40%
            this.emergencyThreshold = 0.55; // 55%
            this.panicThreshold = 0.7;      // 70%
            this.checkInterval = 500;
        } else {
            this.warningThreshold = 0.6;
            this.criticalThreshold = 0.75;
            this.emergencyThreshold = 0.85;
            this.panicThreshold = 0.95;
        }

        this.lastGcTime = 0;
        this.gcCooldown = 1000;
        this.gcAvailable = typeof global.gc === 'function';
        this.monitoringInterval = null;

        console.log(`[${new Date().toISOString()}] INFO: # Chart Memory limit: ${maxMemoryMB}MB`);
        console.log(`[${new Date().toISOString()}] INFO: # Ultra-low memory: ${this.ultraLowMemory ? 'YES' : 'NO'}`);
        console.log(`[${new Date().toISOString()}] INFO: # GC available: ${this.gcAvailable ? 'YES' : 'NO'}`);
        console.log(
            `[${new Date().toISOString()}] INFO: # Thresholds: ${(this.warningThreshold * 100).toFixed(0)}%/${(this.criticalThreshold * 100).toFixed(0)}%/${(this.emergencyThreshold * 100).toFixed(0)}%/${(this.panicThreshold * 100).toFixed(0)}%`
        );

        this.startMonitoring();

        // only handle exit; main() will orchestrate shutdown
        process.on('exit', () => this.cleanup());
    }

    startMonitoring() {
        this.monitoringInterval = setInterval(() => {
            try {
                this.checkMemoryUsage();
            } catch (e) {
                // Prevent monitor from crashing the app
                console.error(`[${new Date().toISOString()}] ERROR: Memory monitor error:`, e && e.message ? e.message : e);
            }
        }, this.checkInterval);
    }

    checkMemoryUsage() {
        const memUsage = process.memoryUsage();
        const usedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
        const usage = memUsage.heapUsed / this.maxMemoryBytes;

        if (usage > this.panicThreshold) {
            console.error(`[${new Date().toISOString()}] PANIC: Chart Memory ${(usage * 100).toFixed(1)}% (${usedMB}MB)`);
            if (usage > 2.0) {
                console.error(`[${new Date().toISOString()}] FATAL: Chart Memory over 200% - forcing exit`);
                process.exit(1);
            }
            return this.panicCleanup();
        } else if (usage > this.emergencyThreshold) {
            console.warn(`[${new Date().toISOString()}] EMERGENCY: Chart Memory ${(usage * 100).toFixed(1)}% (${usedMB}MB)`);
            return this.emergencyCleanup();
        } else if (usage > this.criticalThreshold) {
            console.warn(`[${new Date().toISOString()}] CRITICAL: Chart Memory ${(usage * 100).toFixed(1)}% (${usedMB}MB)`);
            return this.aggressiveCleanup();
        } else if (usage > this.warningThreshold) {
            if (Date.now() % 10000 < this.checkInterval) {
                console.warn(`[${new Date().toISOString()}] WARN: Chart Memory ${(usage * 100).toFixed(1)}% (${usedMB}MB)`);
            }
            this.optimizeMemory();
        }

        if (Date.now() % 30000 < this.checkInterval) {
            console.log(
                `[${new Date().toISOString()}] INFO: # Chart Memory: Heap=${usedMB}MB, Limit=${this.maxMemoryMB}MB`
            );
        }

        return false;
    }

    panicCleanup() {
        if (this.gcAvailable) {
            for (let i = 0; i < 5; i++) global.gc();
        }
        return true;
    }

    emergencyCleanup() {
        if (this.gcAvailable) {
            for (let i = 0; i < 3; i++) global.gc();
        }
        return true;
    }

    aggressiveCleanup() {
        this.forceGarbageCollection();
        return false;
    }

    forceGarbageCollection() {
        const now = Date.now();
        if (now - this.lastGcTime < this.gcCooldown) return;
        if (this.gcAvailable) {
            try {
                global.gc();
                this.lastGcTime = now;
            } catch (e) {
                // ignore GC exceptions
            }
        }
    }

    optimizeMemory() {
        if (this.gcAvailable && Date.now() - this.lastGcTime > this.gcCooldown) {
            try {
                global.gc();
                this.lastGcTime = Date.now();
            } catch (e) {
                // ignore
            }
        }
    }

    cleanup() {
        if (this.monitoringInterval) {
            clearInterval(this.monitoringInterval);
            this.monitoringInterval = null;
        }
        if (this.gcAvailable) {
            try {
                global.gc();
            } catch (e) {
                // ignore
            }
        }
    }

    getMemoryInfo() {
        const memUsage = process.memoryUsage();
        return {
            heapUsedMB: Math.round(memUsage.heapUsed / 1024 / 1024),
            heapTotalMB: Math.round(memUsage.heapTotal / 1024 / 1024),
            usagePercent: (memUsage.heapUsed / this.maxMemoryBytes * 100).toFixed(1),
            ultraLowMemory: this.ultraLowMemory,
            gcAvailable: this.gcAvailable
        };
    }

    isEmergencyState() {
        const memUsage = process.memoryUsage();
        return (memUsage.heapUsed / this.maxMemoryBytes) > this.emergencyThreshold;
    }

    isCriticalState() {
        const memUsage = process.memoryUsage();
        return (memUsage.heapUsed / this.maxMemoryBytes) > this.criticalThreshold;
    }

    isPanicState() {
        const memUsage = process.memoryUsage();
        return (memUsage.heapUsed / this.maxMemoryBytes) > this.panicThreshold;
    }
}

/**
 * TransactionChartGenerator
 * - Uses ComprehensiveTransactionAnalyzer to prepare daily analytics
 * - Draws 3D bar chart and MA lines (original configuration preserved)
 * - Uses MemoryManager above and performs robust final cleanup to ensure Node exits
 */
class TransactionChartGenerator {
    constructor() {
        this.ADDRESS1 = process.env.ADDRESS1;
        this.AGENT = process.env.AGENT ? process.env.AGENT.toLowerCase() : null;
        this.DEBUG = process.env.DEBUG === '1';

        const maxMemoryMB = parseInt(process.env.MAX_MEM_ANALYZE) || 2048;
        this.memoryManager = new MemoryManager(maxMemoryMB);
        this.ultraLowMemory = this.memoryManager.ultraLowMemory;

        // Original chart configuration preserved
        this.chartConfig = {
            width: 1400,
            height: 800,
            padding: {
                top: 100,
                right: 80,
                bottom: 160,
                left: 120
            },
            bar3D: {
                depth: 8,
                shadowOffset: 3
            },
            colors: {
                background: '#ffffff',
                grid: '#e0e0e0',
                totalTx: '#ff8c00',
                totalTxTop: '#ffaa33',
                totalTxRight: '#cc6600',
                address1Tx: '#0066cc',
                address1TxTop: '#3388dd',
                address1TxRight: '#004499',
                agentTx: '#808080',
                agentTxTop: '#999999',
                agentTxRight: '#666666',
                text: '#333333',
                axis: '#666666',
                percentage: '#0066cc',
                movingAvgBlue: 'rgba(0, 51, 102, 0.33)',
                movingAvgOrange: 'rgba(204, 68, 0, 0.33)',
                shadow: 'rgba(0, 0, 0, 0.2)'
            },
            fonts: {
                title: '24px Arial',
                axis: '14px Arial',
                label: '12px Arial',
                legend: '16px Arial',
                percentage: '10px Arial',
                weekday: '10px Arial'
            }
        };

        if (!this.ADDRESS1) throw new Error('ADDRESS1 environment variable is required');

        console.log(`[${new Date().toISOString()}] INFO: # Chart generator initialized`);
        console.log(`[${new Date().toISOString()}] INFO: # Ultra-low memory mode: ${this.ultraLowMemory ? 'YES' : 'NO'}`);
        console.log(`[${new Date().toISOString()}] INFO: # Canvas size: ${this.chartConfig.width}x${this.chartConfig.height}`);
        console.log(`[${new Date().toISOString()}] INFO: # Target address: ${this.ADDRESS1}`);
        if (this.AGENT) console.log(`[${new Date().toISOString()}] INFO: # Agent address: ${this.AGENT}`);
        console.log(`[${new Date().toISOString()}] INFO: # Debug mode: ${this.DEBUG ? 'ENABLED' : 'DISABLED'}`);
    }

    debugLog(message) {
        if (this.DEBUG) console.log(`[${new Date().toISOString()}] DEBUG: ${message}`);
    }

    optimizeMemory() {
        this.memoryManager.optimizeMemory();
    }

    aggressiveMemoryCleanup() {
        if (this.tempData) this.tempData = null;
        if (this.tempCanvas) this.tempCanvas = null;
        if (this.tempContext) this.tempContext = null;
        this.memoryManager.aggressiveCleanup();
    }

    async generateChart(csvFilename, outputFilename = null) {
        try {
            if (this.memoryManager.isCriticalState()) {
                console.warn(`[${new Date().toISOString()}] WARN: # Starting chart generation in critical memory state`);
                this.aggressiveMemoryCleanup();
            }

            const analyzer = new ComprehensiveTransactionAnalyzer(csvFilename);
            await analyzer.init();

            this.optimizeMemory();

            await analyzer.analyzeStreaming();

            this.aggressiveMemoryCleanup();

            const chartData = this.prepare28DaysData(analyzer);

            if (typeof analyzer.cleanup === 'function') {
                try { analyzer.cleanup(); } catch (e) { /* ignore */ }
            }

            if (chartData.length === 0) throw new Error('No data available for chart generation');

            if (!outputFilename) {
                const timestamp = new Date().toISOString().replace(/[T:]/g, '_').replace(/\..+/, '');
                outputFilename = `transaction_chart_${timestamp}.png`;
            }

            this.aggressiveMemoryCleanup();

            if (this.memoryManager.isPanicState()) {
                throw new Error('Memory limit exceeded before chart creation. Consider increasing MAX_MEM_ANALYZE.');
            }

            await this.createChart(chartData, outputFilename);

            chartData.length = 0;

            const memInfo = this.memoryManager.getMemoryInfo();
            console.log(`[${new Date().toISOString()}] INFO: # Chart completed. Final memory usage: ${memInfo.heapUsedMB}MB (${memInfo.usagePercent}%)`);
            console.log(`[${new Date().toISOString()}] INFO: # Chart generated successfully: ${outputFilename}`);

            return outputFilename;
        } catch (error) {
            this.aggressiveMemoryCleanup();
            throw error;
        }
    }

    prepare28DaysData(analyzer) {
        const chartData = [];
        const now = new Date();
        const daysToSkip = 1;
        const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));

        console.log(`[${new Date().toISOString()}] INFO: # Skipping ${daysToSkip} day(s) to avoid incomplete data`);
        console.log(`[${new Date().toISOString()}] INFO: # Current UTC time: ${now.toISOString()}`);

        for (let i = 28 + daysToSkip - 1; i >= daysToSkip; i--) {
            const date = new Date(today);
            date.setUTCDate(today.getUTCDate() - i);
            const dateStr = date.toISOString().split('T')[0];

            const dayStats = analyzer.dailyAnalytics.get(dateStr);
            const totalTxForDay = analyzer.getTxCountForDay(dateStr);

            let agentTx = 0;
            if (this.AGENT && dayStats) agentTx = dayStats.satWheel.oneSatAgentTx || 0;

            const dataPoint = {
                date: dateStr,
                dateObj: new Date(date),
                totalNetworkTx: totalTxForDay,
                address1Tx: dayStats ? dayStats.totalTransactions : 0,
                agentTx,
                sharePercentage: totalTxForDay > 0 ? ((dayStats ? dayStats.totalTransactions : 0) / totalTxForDay * 100) : 0,
                weekday: this.getWeekdayShort(date)
            };

            chartData.push(dataPoint);

            this.debugLog(`# ${dateStr} (${dataPoint.weekday}): Network=${totalTxForDay}, Address1=${dataPoint.address1Tx}, Agent=${dataPoint.agentTx}, Share=${dataPoint.sharePercentage.toFixed(2)}%`);

            if (chartData.length % 7 === 0) {
                this.optimizeMemory();
                if (this.memoryManager.isEmergencyState()) {
                    console.warn(`[${new Date().toISOString()}] EMERGENCY: # Memory critical during data preparation`);
                    this.aggressiveMemoryCleanup();
                }
            }
        }

        this.optimizeMemory();
        console.log(`[${new Date().toISOString()}] INFO: # Prepared ${chartData.length} complete days of chart data`);
        return chartData;
    }

    getWeekdayShort(date) {
        const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        return weekdays[date.getUTCDay()];
    }

    getRoundYAxisValue(maxValue) {
        if (maxValue <= 1000) return Math.ceil(maxValue / 100) * 100;
        if (maxValue <= 10000) return Math.ceil(maxValue / 1000) * 1000;
        if (maxValue <= 100000) return Math.ceil(maxValue / 10000) * 10000;
        if (maxValue <= 1000000) return Math.ceil(maxValue / 100000) * 100000;
        return Math.ceil(maxValue / 1000000) * 1000000;
    }

    calculateMovingAverages(data) {
        const movingAverages = [];
        for (let i = 0; i < data.length; i++) {
            const startIndex = Math.max(0, i - 6);
            const window = data.slice(startIndex, i + 1);
            const avgAddress1 = window.reduce((sum, d) => sum + d.address1Tx, 0) / window.length;
            const avgTotalNetwork = window.reduce((sum, d) => sum + d.totalNetworkTx, 0) / window.length;
            movingAverages.push({ index: i, avgAddress1, avgTotalNetwork });

            this.debugLog(`# Day ${i}: MA7 Address1=${avgAddress1.toFixed(1)}, MA7 Total=${avgTotalNetwork.toFixed(1)} (window size: ${window.length})`);
            if (i % 10 === 0 && i > 0) this.optimizeMemory();
        }
        return movingAverages;
    }

    async createChart(data, filename) {
        let canvas = null;
        let ctx = null;
        try {
            console.log(`[${new Date().toISOString()}] INFO: # Creating canvas for chart generation...`);
            canvas = createCanvas(this.chartConfig.width, this.chartConfig.height);
            ctx = canvas.getContext('2d');

            const memInfo = this.memoryManager.getMemoryInfo();
            console.log(`[${new Date().toISOString()}] INFO: # Canvas created. Memory usage: ${memInfo.heapUsedMB}MB (${memInfo.usagePercent}%)`);

            if (this.memoryManager.isCriticalState()) {
                console.warn(`[${new Date().toISOString()}] WARN: # Critical memory state during canvas creation`);
                this.aggressiveMemoryCleanup();
            }

            ctx.fillStyle = this.chartConfig.colors.background;
            ctx.fillRect(0, 0, canvas.width, canvas.height);

            const chartArea = {
                x: this.chartConfig.padding.left,
                y: this.chartConfig.padding.top,
                width: canvas.width - this.chartConfig.padding.left - this.chartConfig.padding.right,
                height: canvas.height - this.chartConfig.padding.top - this.chartConfig.padding.bottom
            };

            this.optimizeMemory();

            const maxTotalTx = Math.max(...data.map(d => d.totalNetworkTx));
            const maxAddress1Tx = Math.max(...data.map(d => d.address1Tx));
            const maxAgentTx = Math.max(...data.map(d => d.agentTx));
            const rawMaxValue = Math.max(maxTotalTx, maxAddress1Tx, maxAgentTx);
            const maxValue = this.getRoundYAxisValue(rawMaxValue);

            console.log(`[${new Date().toISOString()}] INFO: # Calculating moving averages...`);
            const movingAverages = this.calculateMovingAverages(data);

            this.optimizeMemory();

            this.debugLog(`# Chart ranges: MaxTotal=${maxTotalTx}, MaxAddress1=${maxAddress1Tx}, MaxAgent=${maxAgentTx}, RoundedMax=${maxValue}`);

            ctx.fillStyle = this.chartConfig.colors.text;
            ctx.font = this.chartConfig.fonts.title;
            ctx.textAlign = 'center';
            const title = `${data.length}-Day Transaction Comparison - Total Network vs ${this.ADDRESS1.substring(0, 10)}...${this.AGENT ? ' vs Agent' : ''}`;
            ctx.fillText(title, canvas.width / 2, 40);

            this.drawGrid(ctx, chartArea, maxValue, data.length);

            this.optimizeMemory();

            if (this.memoryManager.isEmergencyState()) {
                console.warn(`[${new Date().toISOString()}] EMERGENCY: # Memory critical before drawing bars`);
                this.aggressiveMemoryCleanup();
            }

            console.log(`[${new Date().toISOString()}] INFO: # Drawing 3D bars...`);
            this.draw3DBars(ctx, chartArea, data, maxValue);

            this.optimizeMemory();

            console.log(`[${new Date().toISOString()}] INFO: # Drawing moving average lines...`);
            this.drawMovingAverageLines(ctx, chartArea, maxValue, movingAverages);

            this.optimizeMemory();

            console.log(`[${new Date().toISOString()}] INFO: # Drawing axes labels...`);
            this.drawAxesLabels(ctx, chartArea, maxValue, data, canvas);

            console.log(`[${new Date().toISOString()}] INFO: # Drawing legend...`);
            this.drawLegend(ctx);

            this.optimizeMemory();

            movingAverages.length = 0;

            console.log(`[${new Date().toISOString()}] INFO: # Saving chart to file...`);
            const buffer = canvas.toBuffer('image/png');
            await fs.promises.writeFile(filename, buffer);

            console.log(`[${new Date().toISOString()}] INFO: # Chart saved as: ${filename}`);
        } finally {
            if (canvas) {
                try { canvas.width = 1; canvas.height = 1; } catch (e) { /* ignore */ }
            }
            ctx = null;
            canvas = null;
            this.aggressiveMemoryCleanup();
        }
    }

    drawGrid(ctx, chartArea, maxValue, dataPoints) {
        ctx.strokeStyle = this.chartConfig.colors.grid;
        ctx.lineWidth = 1;

        const ySteps = 10;
        for (let i = 0; i <= ySteps; i++) {
            const y = chartArea.y + (chartArea.height * i / ySteps);
            ctx.beginPath();
            ctx.moveTo(chartArea.x, y);
            ctx.lineTo(chartArea.x + chartArea.width, y);
            ctx.stroke();
        }

        ctx.strokeStyle = this.chartConfig.colors.axis;
        ctx.lineWidth = 2;

        ctx.beginPath();
        ctx.moveTo(chartArea.x, chartArea.y);
        ctx.lineTo(chartArea.x, chartArea.y + chartArea.height);
        ctx.stroke();

        ctx.beginPath();
        ctx.moveTo(chartArea.x, chartArea.y + chartArea.height);
        ctx.lineTo(chartArea.x + chartArea.width, chartArea.y + chartArea.height);
        ctx.stroke();
    }

    draw3DBars(ctx, chartArea, data, maxValue) {
        if (data.length === 0) return;

        const barWidth = chartArea.width / data.length * 0.8;
        const barSpacing = chartArea.width / data.length * 0.2;
        const depth = this.chartConfig.bar3D.depth;
        const shadowOffset = this.chartConfig.bar3D.shadowOffset;

        for (let i = 0; i < data.length; i++) {
            const x = chartArea.x + (chartArea.width * i / data.length) + (barSpacing / 2);

            const totalTxValue = data[i].totalNetworkTx;
            const address1TxValue = data[i].address1Tx;
            const agentTxValue = data[i].agentTx;

            const totalTxHeight = (chartArea.height * totalTxValue / maxValue);
            const address1TxHeight = (chartArea.height * address1TxValue / maxValue);
            const agentTxHeight = (chartArea.height * agentTxValue / maxValue);

            if (totalTxValue > 0 || address1TxValue > 0 || agentTxValue > 0) {
                const maxHeight = Math.max(totalTxHeight, address1TxHeight, agentTxHeight);
                const shadowY = chartArea.y + chartArea.height - maxHeight + shadowOffset;
                ctx.fillStyle = this.chartConfig.colors.shadow;
                ctx.fillRect(x + shadowOffset, shadowY, barWidth, maxHeight);
            }

            if (totalTxValue > 0) {
                const totalBarY = chartArea.y + chartArea.height - totalTxHeight;
                this.draw3DBar(ctx, x, totalBarY, barWidth, totalTxHeight, depth,
                    this.chartConfig.colors.totalTx,
                    this.chartConfig.colors.totalTxTop,
                    this.chartConfig.colors.totalTxRight);
            }

            if (address1TxValue > 0) {
                const address1BarY = chartArea.y + chartArea.height - address1TxHeight;
                this.draw3DBar(ctx, x, address1BarY, barWidth, address1TxHeight, depth,
                    this.chartConfig.colors.address1Tx,
                    this.chartConfig.colors.address1TxTop,
                    this.chartConfig.colors.address1TxRight);

                const percentage = data[i].sharePercentage.toFixed(1);
                if (parseFloat(percentage) > 0) {
                    ctx.fillStyle = this.chartConfig.colors.percentage;
                    ctx.font = this.chartConfig.fonts.percentage;
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'bottom';
                    const textY = address1BarY - depth - 2;
                    ctx.fillText(`${percentage}%`, x + (barWidth / 2), textY);
                }
            }

            if (agentTxValue > 0 && this.AGENT) {
                const agentBarY = chartArea.y + chartArea.height - agentTxHeight;
                this.draw3DBar(ctx, x, agentBarY, barWidth, agentTxHeight, depth,
                    this.chartConfig.colors.agentTx,
                    this.chartConfig.colors.agentTxTop,
                    this.chartConfig.colors.agentTxRight);
            }

            if (i % 7 === 0 && i > 0) {
                this.optimizeMemory();
                if (this.memoryManager.isEmergencyState()) {
                    console.warn(`[${new Date().toISOString()}] EMERGENCY: # Memory critical during bar drawing at bar ${i}`);
                    this.aggressiveMemoryCleanup();
                }
            }
        }
    }

    draw3DBar(ctx, x, y, width, height, depth, frontColor, topColor, rightColor) {
        ctx.fillStyle = frontColor;
        ctx.fillRect(x, y, width, height);

        ctx.fillStyle = topColor;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.fill();

        ctx.fillStyle = rightColor;
        ctx.beginPath();
        ctx.moveTo(x + width, y);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width + depth, y + height - depth);
        ctx.lineTo(x + width, y + height);
        ctx.closePath();
        ctx.fill();

        ctx.strokeStyle = 'rgba(0, 0, 0, 0.1)';
        ctx.lineWidth = 0.5;

        ctx.strokeRect(x, y, width, height);
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.stroke();

        ctx.beginPath();
        ctx.moveTo(x + width, y);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width + depth, y + height - depth);
        ctx.lineTo(x + width, y + height);
        ctx.closePath();
        ctx.stroke();
    }

    drawMovingAverageLines(ctx, chartArea, maxValue, movingAverages) {
        if (movingAverages.length < 2) return;

        const bluePoints = [];
        const orangePoints = [];

        for (let i = 0; i < movingAverages.length; i++) {
            const x = chartArea.x + (chartArea.width * i / (movingAverages.length - 1));
            const blueValue = movingAverages[i].avgAddress1;
            const blueY = chartArea.y + chartArea.height - (chartArea.height * blueValue / maxValue);
            bluePoints.push({ x, y: blueY });

            const orangeValue = movingAverages[i].avgTotalNetwork;
            const orangeY = chartArea.y + chartArea.height - (chartArea.height * orangeValue / maxValue);
            orangePoints.push({ x, y: orangeY });

            if (i % 10 === 0 && i > 0) this.optimizeMemory();
        }

        this.drawSmoothCurve(ctx, bluePoints, this.chartConfig.colors.movingAvgBlue);
        this.optimizeMemory();
        this.drawSmoothCurve(ctx, orangePoints, this.chartConfig.colors.movingAvgOrange);

        bluePoints.length = 0;
        orangePoints.length = 0;
    }

    drawSmoothCurve(ctx, points, color) {
        if (points.length < 2) return;

        ctx.strokeStyle = color;
        ctx.lineWidth = 4;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.beginPath();

        ctx.moveTo(points[0].x, points[0].y);

        if (points.length === 2) {
            ctx.lineTo(points[1].x, points[1].y);
        } else {
            for (let i = 1; i < points.length - 1; i++) {
                const current = points[i];
                const next = points[i + 1];
                const controlX = (current.x + next.x) / 2;
                const controlY = (current.y + next.y) / 2;
                ctx.quadraticCurveTo(current.x, current.y, controlX, controlY);
            }
            const lastPoint = points[points.length - 1];
            const secondLastPoint = points[points.length - 2];
            ctx.quadraticCurveTo(secondLastPoint.x, secondLastPoint.y, lastPoint.x, lastPoint.y);
        }

        ctx.stroke();
    }

    drawAxesLabels(ctx, chartArea, maxValue, data, canvas) {
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.font = this.chartConfig.fonts.axis;

        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        const ySteps = 10;
        for (let i = 0; i <= ySteps; i++) {
            const value = Math.round(maxValue * (ySteps - i) / ySteps);
            const y = chartArea.y + (chartArea.height * i / ySteps);
            ctx.fillText(value.toLocaleString(), chartArea.x - 10, y);
        }

        ctx.save();
        ctx.translate(30, chartArea.y + chartArea.height / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.textAlign = 'center';
        ctx.font = this.chartConfig.fonts.legend;
        ctx.fillText('Transactions per Day', 0, 0);
        ctx.restore();

        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';

        for (let i = 0; i < data.length; i++) {
            const x = chartArea.x + (chartArea.width * i / data.length) + (chartArea.width / data.length / 2);
            const date = data[i].dateObj;
            const dateStr = `${(date.getUTCMonth() + 1).toString().padStart(2, '0')}/${date.getUTCDate().toString().padStart(2, '0')}`;
            const weekday = data[i].weekday;

            ctx.font = this.chartConfig.fonts.weekday;
            ctx.fillStyle = this.chartConfig.colors.axis;
            ctx.fillText(weekday, x, chartArea.y + chartArea.height + 5);

            ctx.font = this.chartConfig.fonts.label;
            ctx.fillStyle = this.chartConfig.colors.text;
            ctx.save();
            ctx.translate(x, chartArea.y + chartArea.height + 25);
            ctx.rotate(-Math.PI / 4);
            ctx.textAlign = 'right';
            ctx.fillText(dateStr, 0, 0);
            ctx.restore();

            if (i % 7 === 0 && i > 0) this.optimizeMemory();
        }

        ctx.textAlign = 'center';
        ctx.font = this.chartConfig.fonts.legend;
        ctx.fillStyle = this.chartConfig.colors.text;

        ctx.font = this.chartConfig.fonts.label;
        const now = new Date();
        const currentDateStr = `Generated: ${now.toISOString().split('T')[0]} ${now.toTimeString().split(' ')[0]} UTC`;
        ctx.textAlign = 'right';
        ctx.fillText(currentDateStr, canvas.width - 10, canvas.height - 10);
    }

    drawLegend(ctx) {
        const legendX = this.chartConfig.padding.left + 50;
        const legendY = this.chartConfig.height - 50;

        ctx.font = this.chartConfig.fonts.legend;

        this.draw3DLegendBar(ctx, legendX, legendY, 20, 15, 3,
            this.chartConfig.colors.totalTx,
            this.chartConfig.colors.totalTxTop,
            this.chartConfig.colors.totalTxRight);
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText('Total Network Transactions', legendX + 30, legendY + 7);

        const address1LegendX = legendX + 280;
        this.draw3DLegendBar(ctx, address1LegendX, legendY, 20, 15, 3,
            this.chartConfig.colors.address1Tx,
            this.chartConfig.colors.address1TxTop,
            this.chartConfig.colors.address1TxRight);
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText(`${this.ADDRESS1.substring(0, 10)}... Transactions`, address1LegendX + 30, legendY + 7);

        if (this.AGENT) {
            const agentLegendX = address1LegendX + 280;
            this.draw3DLegendBar(ctx, agentLegendX, legendY, 20, 15, 3,
                this.chartConfig.colors.agentTx,
                this.chartConfig.colors.agentTxTop,
                this.chartConfig.colors.agentTxRight);
            ctx.fillStyle = this.chartConfig.colors.text;
            ctx.fillText(`${this.AGENT.substring(0, 10)}... (Agent) Transactions`, agentLegendX + 30, legendY + 7);
        }

        const maLegendY = legendY - 20;

        ctx.strokeStyle = this.chartConfig.colors.movingAvgBlue;
        ctx.lineWidth = 4;
        ctx.lineCap = 'round';
        ctx.beginPath();
        ctx.moveTo(legendX, maLegendY + 7);
        ctx.lineTo(legendX + 20, maLegendY + 7);
        ctx.stroke();
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText('7-Day MA (Address)', legendX + 30, maLegendY + 7);

        const maOrangeLegendX = legendX + 200;
        ctx.strokeStyle = this.chartConfig.colors.movingAvgOrange;
        ctx.lineWidth = 4;
        ctx.lineCap = 'round';
        ctx.beginPath();
        ctx.moveTo(maOrangeLegendX, maLegendY + 7);
        ctx.lineTo(maOrangeLegendX + 20, maLegendY + 7);
        ctx.stroke();
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText('7-Day MA (Total Network)', maOrangeLegendX + 30, maLegendY + 7);

        ctx.font = this.chartConfig.fonts.label;
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText(`Address: ${this.ADDRESS1}`, legendX, legendY + 25);
        if (this.AGENT) ctx.fillText(`Agent: ${this.AGENT}`, legendX + 400, legendY + 25);
        ctx.fillText(`Blue percentages above bars represent share of total network transactions`, legendX, legendY - 35);
    }

    draw3DLegendBar(ctx, x, y, width, height, depth, frontColor, topColor, rightColor) {
        ctx.fillStyle = frontColor;
        ctx.fillRect(x, y, width, height);

        ctx.fillStyle = topColor;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.fill();

        ctx.fillStyle = rightColor;
        ctx.beginPath();
        ctx.moveTo(x + width, y);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width + depth, y + height - depth);
        ctx.lineTo(x + width, y + height);
        ctx.closePath();
        ctx.fill();
    }
}

/**
 * Force-close remaining active handles that commonly keep node running.
 * This is a pragmatic emergency cleanup to ensure process terminates.
 * It is defensive and skips stdin/stdout/stderr.
 */
async function forceExitCleanup() {
    try {
        if (global.gc) {
            try { global.gc(); } catch (e) { /* ignore */ }
        }
    } catch (e) { /* ignore */ }

    // Give a small moment for GC to run
    await new Promise(resolve => setTimeout(resolve, 50));

    if (typeof process._getActiveHandles === 'function') {
        try {
            const handles = process._getActiveHandles();
            for (const h of handles) {
                try {
                    // skip core stdio streams
                    if (h === process.stdin || h === process.stdout || h === process.stderr) continue;

                    // Timer-like objects
                    if (h && h.constructor && (h.constructor.name === 'Timeout' || h.constructor.name === 'Immediate')) {
                        try { clearInterval(h); } catch (e) { /* ignore */ }
                        try { clearTimeout(h); } catch (e) { /* ignore */ }
                        continue;
                    }

                    // Server/socket-like objects
                    if (h && typeof h.close === 'function') {
                        try { h.close(); } catch (e) { /* ignore */ }
                        continue;
                    }

                    if (h && typeof h.destroy === 'function') {
                        try { h.destroy(); } catch (e) { /* ignore */ }
                        continue;
                    }
                } catch (e) {
                    // ignore per-handle errors
                }
            }
        } catch (e) {
            // ignore enumeration errors
        }
    }

    // small delay to let close() complete
    await new Promise(resolve => setTimeout(resolve, 50));
}

// Main execution function with robust cleanup and guaranteed exit
async function main() {
    let exitCode = 0;
    let chartGenerator = null;

    try {
        const csvFilename = process.argv[2] || process.env.CSV_FILENAME;
        const outputFilename = process.argv[3];

        if (!csvFilename) {
            console.error(`[${new Date().toISOString()}] ERROR: ## Please provide CSV filename as argument or set CSV_FILENAME environment variable`);
            console.error(`[${new Date().toISOString()}] INFO: ## Usage: node analyzeChart.js <csv_filename> [output_filename.png]`);
            exitCode = 1;
            return;
        }

        if (!process.env.ADDRESS1) {
            console.error(`[${new Date().toISOString()}] ERROR: ## ADDRESS1 environment variable is required`);
            exitCode = 1;
            return;
        }

        try {
            fs.accessSync(csvFilename);
        } catch (err) {
            console.error(`[${new Date().toISOString()}] ERROR: ## CSV file not found: ${csvFilename}`);
            exitCode = 1;
            return;
        }

        chartGenerator = new TransactionChartGenerator();
        const startTime = Date.now();

        console.log(`[${new Date().toISOString()}] INFO: ## Starting chart generation with memory management...`);

        const out = await chartGenerator.generateChart(csvFilename, outputFilename);

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`[${new Date().toISOString()}] INFO: ## Chart generation completed in ${duration} seconds`);
        console.log(`[${new Date().toISOString()}] INFO: ## Chart saved as: ${out}`);

        // Print final memory info
        if (chartGenerator && chartGenerator.memoryManager) {
            try {
                const memInfo = chartGenerator.memoryManager.getMemoryInfo();
                console.log(`[${new Date().toISOString()}] INFO: ## Final memory usage: ${memInfo.heapUsedMB}MB (${memInfo.usagePercent}%) of ${chartGenerator.memoryManager.maxMemoryMB}MB limit`);
            } catch (e) { /* ignore */ }
        }
    } catch (err) {
        console.error(`[${new Date().toISOString()}] ERROR: ## Chart generation failed:`, err && err.message ? err.message : err);
        exitCode = 1;
    } finally {
        try {
            // 1) Ask chart generator to run internal cleanup
            try {
                if (chartGenerator && chartGenerator.memoryManager && typeof chartGenerator.memoryManager.cleanup === 'function') {
                    chartGenerator.memoryManager.cleanup();
                }
            } catch (e) { /* ignore */ }

            try {
                if (chartGenerator && typeof chartGenerator.aggressiveMemoryCleanup === 'function') {
                    chartGenerator.aggressiveMemoryCleanup();
                }
            } catch (e) { /* ignore */ }

            // 2) Attempt to cleanup any lingering handles (timers, sockets, etc.)
            await forceExitCleanup();

            // 3) Final GC and tiny delay to flush stdout/stderr
            if (global.gc) {
                try { global.gc(); } catch (e) { /* ignore */ }
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        } catch (cleanupError) {
            console.error(`[${new Date().toISOString()}] WARN: ## Cleanup error (non-fatal):`, cleanupError && cleanupError.message ? cleanupError.message : cleanupError);
        } finally {
            // Guaranteed process termination
            try {
                console.log(`[${new Date().toISOString()}] INFO: ## Exiting with code ${exitCode}`);
            } catch (e) { /* ignore */ }
            // ensure exit
            process.exit(exitCode);
        }
    }
}

// Export the class for use in other modules
module.exports = { TransactionChartGenerator };

// Run main function if executed directly
if (require.main === module) {
    // make sure any uncaught exceptions cause exit
    process.on('uncaughtException', (err) => {
        console.error(`[${new Date().toISOString()}] FATAL: Uncaught exception:`, err && err.message ? err.message : err);
        // allow main finally to run on next tick
        setImmediate(() => process.exit(1));
    });
    process.on('unhandledRejection', (reason) => {
        console.error(`[${new Date().toISOString()}] FATAL: Unhandled rejection:`, reason);
        setImmediate(() => process.exit(1));
    });

    main().catch((err) => {
        console.error(`[${new Date().toISOString()}] FATAL: Unhandled error in main:`, err && err.message ? err.message : err);
        process.exit(1);
    });
}