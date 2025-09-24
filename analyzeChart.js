const fs = require('fs');
const path = require('path');
const { createCanvas } = require('canvas');
const { ComprehensiveTransactionAnalyzer } = require('./analyze.js');

require('dotenv').config();

class TransactionChartGenerator {
    constructor() {
        this.ADDRESS1 = process.env.ADDRESS1;
        this.AGENT = process.env.AGENT ? process.env.AGENT.toLowerCase() : null;
        this.DEBUG = process.env.DEBUG === '1';
        
        // Chart configuration
        this.chartConfig = {
            width: 1400, // Wider for bar chart
            height: 800,
            padding: {
                top: 100, // Increased for percentage labels above bars
                right: 80,
                bottom: 160, // Increased for weekday labels and more space
                left: 120
            },
            // 3D effect settings
            bar3D: {
                depth: 8, // 3D depth in pixels
                shadowOffset: 3 // Shadow offset
            },
            colors: {
                background: '#ffffff',
                grid: '#e0e0e0',
                totalTx: '#ff8c00', // Orange bars
                totalTxTop: '#ffaa33', // Lighter orange for top face
                totalTxRight: '#cc6600', // Darker orange for right face
                address1Tx: '#0066cc', // Blue bars
                address1TxTop: '#3388dd', // Lighter blue for top face
                address1TxRight: '#004499', // Darker blue for right face
                agentTx: '#808080', // Grey bars for agent transactions
                agentTxTop: '#999999', // Lighter grey for top face
                agentTxRight: '#666666', // Darker grey for right face
                text: '#333333',
                axis: '#666666',
                percentage: '#0066cc', // Blue text for percentage above bars
                movingAvgBlue: 'rgba(0, 51, 102, 0.33)', // 20% opacity dark blue
                movingAvgOrange: 'rgba(204, 68, 0, 0.33)', // 20% opacity dark orange
                shadow: 'rgba(0, 0, 0, 0.2)' // Shadow color
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
        
        if (!this.ADDRESS1) {
            throw new Error('ADDRESS1 environment variable is required');
        }
        
        console.log(`[${new Date().toISOString()}] INFO: # Chart generator initialized`);
        console.log(`[${new Date().toISOString()}] INFO: # Target address: ${this.ADDRESS1}`);
        if (this.AGENT) {
            console.log(`[${new Date().toISOString()}] INFO: # Agent address: ${this.AGENT}`);
        } else {
            console.log(`[${new Date().toISOString()}] INFO: # No agent address configured`);
        }
        console.log(`[${new Date().toISOString()}] INFO: # Debug mode: ${this.DEBUG ? 'ENABLED' : 'DISABLED'}`);
    }

    debugLog(message) {
        if (this.DEBUG) {
            console.log(`[${new Date().toISOString()}] DEBUG: ${message}`);
        }
    }

    async generateChart(csvFilename, outputFilename = null) {
        try {
            // Initialize analyzer
            const analyzer = new ComprehensiveTransactionAnalyzer(csvFilename);
            await analyzer.init();
            
            console.log(`[${new Date().toISOString()}] INFO: # Starting analysis for chart generation...`);
            await analyzer.analyzeStreaming();
            
            // Get 28 days of data (excluding incomplete days)
            const chartData = this.prepare28DaysData(analyzer);
            
            if (chartData.length === 0) {
                throw new Error('No data available for chart generation');
            }
            
            // Generate output filename if not provided
            if (!outputFilename) {
                const timestamp = new Date().toISOString()
                    .replace(/[T:]/g, '_')
                    .replace(/\..+/, '');
                outputFilename = `transaction_chart_${timestamp}.png`;
            }
            
            // Create the chart
            await this.createChart(chartData, outputFilename);
            
            console.log(`[${new Date().toISOString()}] INFO: # Chart generated successfully: ${outputFilename}`);
            return outputFilename;
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: # Chart generation failed:`, error);
            throw error;
        }
    }

    prepare28DaysData(analyzer) {
        const chartData = [];
        
        // Get current date and time
        const now = new Date();
        
        // Calculate how many complete days we should go back
        const daysToSkip = 1; // Always skip today as it's incomplete
        
        const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
        
        console.log(`[${new Date().toISOString()}] INFO: # Skipping ${daysToSkip} day(s) to avoid incomplete data`);
        console.log(`[${new Date().toISOString()}] INFO: # Current UTC time: ${now.toISOString()}`);
        
        // Get last 28 complete days (excluding today)
        for (let i = 28 + daysToSkip - 1; i >= daysToSkip; i--) {
            const date = new Date(today);
            date.setUTCDate(today.getUTCDate() - i);
            const dateStr = date.toISOString().split('T')[0];
            
            // Get daily analytics for this date
            const dayStats = analyzer.dailyAnalytics.get(dateStr);
            const totalTxForDay = analyzer.getTxCountForDay(dateStr);
            
            // Calculate agent transactions if agent is configured
            let agentTx = 0;
            if (this.AGENT && dayStats) {
                // Agent transactions are tracked in satWheel.oneSatAgentTx
                agentTx = dayStats.satWheel.oneSatAgentTx || 0;
            }
            
            const dataPoint = {
                date: dateStr,
                dateObj: new Date(date),
                totalNetworkTx: totalTxForDay,
                address1Tx: dayStats ? dayStats.totalTransactions : 0,
                agentTx: agentTx,
                sharePercentage: totalTxForDay > 0 ? ((dayStats ? dayStats.totalTransactions : 0) / totalTxForDay * 100) : 0,
                weekday: this.getWeekdayShort(date)
            };
            
            chartData.push(dataPoint);
            
            this.debugLog(`# ${dateStr} (${dataPoint.weekday}): Network=${totalTxForDay}, Address1=${dataPoint.address1Tx}, Agent=${dataPoint.agentTx}, Share=${dataPoint.sharePercentage.toFixed(2)}%`);
        }
        
        console.log(`[${new Date().toISOString()}] INFO: # Prepared ${chartData.length} complete days of chart data`);
        return chartData;
    }

    // Helper function to get short weekday names
    getWeekdayShort(date) {
        const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        return weekdays[date.getUTCDay()];
    }

    // Helper function to round numbers nicely for Y-axis
    getRoundYAxisValue(maxValue) {
        // Find appropriate rounding based on max value
        if (maxValue <= 1000) return Math.ceil(maxValue / 100) * 100;
        if (maxValue <= 10000) return Math.ceil(maxValue / 1000) * 1000;
        if (maxValue <= 100000) return Math.ceil(maxValue / 10000) * 10000;
        if (maxValue <= 1000000) return Math.ceil(maxValue / 100000) * 100000;
        return Math.ceil(maxValue / 1000000) * 1000000;
    }

    // Calculate 7-day moving averages
    calculateMovingAverages(data) {
        const movingAverages = [];
        
        for (let i = 0; i < data.length; i++) {
            // For 7-day moving average, use current day and previous 6 days
            const startIndex = Math.max(0, i - 6);
            const endIndex = i + 1;
            const window = data.slice(startIndex, endIndex);
            
            const avgAddress1 = window.reduce((sum, d) => sum + d.address1Tx, 0) / window.length;
            const avgTotalNetwork = window.reduce((sum, d) => sum + d.totalNetworkTx, 0) / window.length;
            
            movingAverages.push({
                index: i,
                avgAddress1: avgAddress1,
                avgTotalNetwork: avgTotalNetwork
            });
            
            this.debugLog(`# Day ${i}: MA7 Address1=${avgAddress1.toFixed(1)}, MA7 Total=${avgTotalNetwork.toFixed(1)} (window size: ${window.length})`);
        }
        
        return movingAverages;
    }

    async createChart(data, filename) {
        const canvas = createCanvas(this.chartConfig.width, this.chartConfig.height);
        const ctx = canvas.getContext('2d');
        
        // Clear background
        ctx.fillStyle = this.chartConfig.colors.background;
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        
        // Calculate chart area
        const chartArea = {
            x: this.chartConfig.padding.left,
            y: this.chartConfig.padding.top,
            width: canvas.width - this.chartConfig.padding.left - this.chartConfig.padding.right,
            height: canvas.height - this.chartConfig.padding.top - this.chartConfig.padding.bottom
        };
        
        // Find data ranges and round max value
        const maxTotalTx = Math.max(...data.map(d => d.totalNetworkTx));
        const maxAddress1Tx = Math.max(...data.map(d => d.address1Tx));
        const maxAgentTx = Math.max(...data.map(d => d.agentTx));
        const rawMaxValue = Math.max(maxTotalTx, maxAddress1Tx, maxAgentTx);
        const maxValue = this.getRoundYAxisValue(rawMaxValue);
        
        // Calculate 7-day moving averages
        const movingAverages = this.calculateMovingAverages(data);
        
        this.debugLog(`# Chart ranges: MaxTotal=${maxTotalTx}, MaxAddress1=${maxAddress1Tx}, MaxAgent=${maxAgentTx}, RoundedMax=${maxValue}`);
        
        // Draw title
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.font = this.chartConfig.fonts.title;
        ctx.textAlign = 'center';
        const title = `${data.length}-Day Transaction Comparison - Total Network vs ${this.ADDRESS1.substring(0, 10)}...${this.AGENT ? ' vs Agent' : ''}`;
        ctx.fillText(title, canvas.width / 2, 40);
        
        // Draw grid and axes
        this.drawGrid(ctx, chartArea, maxValue, data.length);
        
        // Draw 3D bars
        this.draw3DBars(ctx, chartArea, data, maxValue);
        
        // Draw 7-day moving average lines
        this.drawMovingAverageLines(ctx, chartArea, maxValue, movingAverages);
        
        // Draw axes labels
        this.drawAxesLabels(ctx, chartArea, maxValue, data, canvas);
        
        // Draw legend (with more space from x-axis)
        this.drawLegend(ctx);
        
        // Save the chart
        const buffer = canvas.toBuffer('image/png');
        await fs.promises.writeFile(filename, buffer);
        
        console.log(`[${new Date().toISOString()}] INFO: # Chart saved as: ${filename}`);
    }

    drawGrid(ctx, chartArea, maxValue, dataPoints) {
        ctx.strokeStyle = this.chartConfig.colors.grid;
        ctx.lineWidth = 1;
        
        // Horizontal grid lines (Y-axis) - use round numbers
        const ySteps = 10;
        for (let i = 0; i <= ySteps; i++) {
            const y = chartArea.y + (chartArea.height * i / ySteps);
            ctx.beginPath();
            ctx.moveTo(chartArea.x, y);
            ctx.lineTo(chartArea.x + chartArea.width, y);
            ctx.stroke();
        }
        
        // Draw main axes
        ctx.strokeStyle = this.chartConfig.colors.axis;
        ctx.lineWidth = 2;
        
        // Y-axis
        ctx.beginPath();
        ctx.moveTo(chartArea.x, chartArea.y);
        ctx.lineTo(chartArea.x, chartArea.y + chartArea.height);
        ctx.stroke();
        
        // X-axis
        ctx.beginPath();
        ctx.moveTo(chartArea.x, chartArea.y + chartArea.height);
        ctx.lineTo(chartArea.x + chartArea.width, chartArea.y + chartArea.height);
        ctx.stroke();
    }

    // Draw 3D bars with depth and shading
    draw3DBars(ctx, chartArea, data, maxValue) {
        if (data.length === 0) return;
        
        const barWidth = chartArea.width / data.length * 0.8; // 80% of available space
        const barSpacing = chartArea.width / data.length * 0.2; // 20% for spacing
        const depth = this.chartConfig.bar3D.depth;
        const shadowOffset = this.chartConfig.bar3D.shadowOffset;
        
        for (let i = 0; i < data.length; i++) {
            const x = chartArea.x + (chartArea.width * i / data.length) + (barSpacing / 2);
            
            const totalTxValue = data[i].totalNetworkTx;
            const address1TxValue = data[i].address1Tx;
            const agentTxValue = data[i].agentTx;
            
            // Calculate bar heights
            const totalTxHeight = (chartArea.height * totalTxValue / maxValue);
            const address1TxHeight = (chartArea.height * address1TxValue / maxValue);
            const agentTxHeight = (chartArea.height * agentTxValue / maxValue);
            
            // Draw shadow first
            if (totalTxValue > 0 || address1TxValue > 0 || agentTxValue > 0) {
                const maxHeight = Math.max(totalTxHeight, address1TxHeight, agentTxHeight);
                const shadowY = chartArea.y + chartArea.height - maxHeight + shadowOffset;
                ctx.fillStyle = this.chartConfig.colors.shadow;
                ctx.fillRect(x + shadowOffset, shadowY, barWidth, maxHeight);
            }
            
            // Draw orange bar (total network transactions) - bottom layer
            if (totalTxValue > 0) {
                const totalBarY = chartArea.y + chartArea.height - totalTxHeight;
                this.draw3DBar(ctx, x, totalBarY, barWidth, totalTxHeight, depth, 
                              this.chartConfig.colors.totalTx, 
                              this.chartConfig.colors.totalTxTop, 
                              this.chartConfig.colors.totalTxRight);
            }
            
            // Draw blue bar (address1 transactions) - middle layer
            if (address1TxValue > 0) {
                const address1BarY = chartArea.y + chartArea.height - address1TxHeight;
                this.draw3DBar(ctx, x, address1BarY, barWidth, address1TxHeight, depth, 
                              this.chartConfig.colors.address1Tx, 
                              this.chartConfig.colors.address1TxTop, 
                              this.chartConfig.colors.address1TxRight);
                
                // Add percentage text ABOVE the blue bar in blue color
                const percentage = data[i].sharePercentage.toFixed(1);
                if (parseFloat(percentage) > 0) { // Only show if percentage is greater than 0
                    ctx.fillStyle = this.chartConfig.colors.percentage; // Blue color
                    ctx.font = this.chartConfig.fonts.percentage;
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'bottom';
                    const textY = address1BarY - depth - 2; // Account for 3D depth
                    ctx.fillText(`${percentage}%`, x + (barWidth / 2), textY);
                }
            }
            
            // Draw grey bar (agent transactions) - top layer
            if (agentTxValue > 0 && this.AGENT) {
                const agentBarY = chartArea.y + chartArea.height - agentTxHeight;
                this.draw3DBar(ctx, x, agentBarY, barWidth, agentTxHeight, depth, 
                              this.chartConfig.colors.agentTx, 
                              this.chartConfig.colors.agentTxTop, 
                              this.chartConfig.colors.agentTxRight);
            }
        }
    }

    // Helper function to draw a single 3D bar
    draw3DBar(ctx, x, y, width, height, depth, frontColor, topColor, rightColor) {
        // Draw front face
        ctx.fillStyle = frontColor;
        ctx.fillRect(x, y, width, height);
        
        // Draw top face (parallelogram)
        ctx.fillStyle = topColor;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.fill();
        
        // Draw right face (parallelogram)
        ctx.fillStyle = rightColor;
        ctx.beginPath();
        ctx.moveTo(x + width, y);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width + depth, y + height - depth);
        ctx.lineTo(x + width, y + height);
        ctx.closePath();
        ctx.fill();
        
        // Add subtle borders for better definition
        ctx.strokeStyle = 'rgba(0, 0, 0, 0.1)';
        ctx.lineWidth = 0.5;
        
        // Border for front face
        ctx.strokeRect(x, y, width, height);
        
        // Border for top face
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.stroke();
        
        // Border for right face
        ctx.beginPath();
        ctx.moveTo(x + width, y);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width + depth, y + height - depth);
        ctx.lineTo(x + width, y + height);
        ctx.closePath();
        ctx.stroke();
    }

    // Draw smooth 7-day moving average lines with cubic Bézier curves
    drawMovingAverageLines(ctx, chartArea, maxValue, movingAverages) {
        if (movingAverages.length < 2) return;
        
        // Create coordinate arrays for both lines
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
        }
        
        // Draw smooth curve for address1 (dark blue with 20% opacity)
        this.drawSmoothCurve(ctx, bluePoints, this.chartConfig.colors.movingAvgBlue);
        
        // Draw smooth curve for total network (dark orange with 20% opacity)
        this.drawSmoothCurve(ctx, orangePoints, this.chartConfig.colors.movingAvgOrange);
    }
    
    // Helper function to draw smooth cubic Bézier curves
    drawSmoothCurve(ctx, points, color) {
        if (points.length < 2) return;
        
        ctx.strokeStyle = color;
        ctx.lineWidth = 4;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.beginPath();
        
        // Move to first point
        ctx.moveTo(points[0].x, points[0].y);
        
        if (points.length === 2) {
            // If only 2 points, draw straight line
            ctx.lineTo(points[1].x, points[1].y);
        } else {
            // For smooth curves, use quadratic curves between points
            for (let i = 1; i < points.length - 1; i++) {
                const current = points[i];
                const next = points[i + 1];
                
                // Calculate control point for smooth curve
                const controlX = (current.x + next.x) / 2;
                const controlY = (current.y + next.y) / 2;
                
                ctx.quadraticCurveTo(current.x, current.y, controlX, controlY);
            }
            
            // Draw final segment to last point
            const lastPoint = points[points.length - 1];
            const secondLastPoint = points[points.length - 2];
            ctx.quadraticCurveTo(secondLastPoint.x, secondLastPoint.y, lastPoint.x, lastPoint.y);
        }
        
        ctx.stroke();
    }

    drawAxesLabels(ctx, chartArea, maxValue, data, canvas) {
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.font = this.chartConfig.fonts.axis;
        
        // Y-axis labels - use round numbers
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        const ySteps = 10;
        for (let i = 0; i <= ySteps; i++) {
            const value = Math.round(maxValue * (ySteps - i) / ySteps);
            const y = chartArea.y + (chartArea.height * i / ySteps);
            ctx.fillText(value.toLocaleString(), chartArea.x - 10, y);
        }
        
        // Y-axis title
        ctx.save();
        ctx.translate(30, chartArea.y + chartArea.height / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.textAlign = 'center';
        ctx.font = this.chartConfig.fonts.legend;
        ctx.fillText('Transactions per Day', 0, 0);
        ctx.restore();
        
        // X-axis labels (show every date in MM/dd format with weekdays)
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        
        for (let i = 0; i < data.length; i++) {
            const x = chartArea.x + (chartArea.width * i / data.length) + (chartArea.width / data.length / 2);
            const date = data[i].dateObj;
            const dateStr = `${(date.getUTCMonth() + 1).toString().padStart(2, '0')}/${date.getUTCDate().toString().padStart(2, '0')}`;
            const weekday = data[i].weekday;
            
            // Draw weekday (smaller font)
            ctx.font = this.chartConfig.fonts.weekday;
            ctx.fillStyle = this.chartConfig.colors.axis;
            ctx.fillText(weekday, x, chartArea.y + chartArea.height + 5);
            
            // Draw date (rotated for better readability)
            ctx.font = this.chartConfig.fonts.label;
            ctx.fillStyle = this.chartConfig.colors.text;
            ctx.save();
            ctx.translate(x, chartArea.y + chartArea.height + 25);
            ctx.rotate(-Math.PI / 4); // 45 degree rotation
            ctx.textAlign = 'right';
            ctx.fillText(dateStr, 0, 0);
            ctx.restore();
        }
        
        // X-axis title
        ctx.textAlign = 'center';
        ctx.font = this.chartConfig.fonts.legend;
        ctx.fillStyle = this.chartConfig.colors.text;
        
        // Add current date info
        ctx.font = this.chartConfig.fonts.label;
        const now = new Date();
        const currentDateStr = `Generated: ${now.toISOString().split('T')[0]} ${now.toTimeString().split(' ')[0]} UTC`;
        ctx.textAlign = 'right';
        ctx.fillText(currentDateStr, canvas.width - 10, canvas.height - 10);
    }

    drawLegend(ctx) {
        // Moved legend further down with more space from x-axis
        const legendX = this.chartConfig.padding.left + 50;
        const legendY = this.chartConfig.height - 50; // More space from bottom
        
        ctx.font = this.chartConfig.fonts.legend;
        
        // Total network transactions legend (orange) - 3D style
        this.draw3DLegendBar(ctx, legendX, legendY, 20, 15, 3, 
                           this.chartConfig.colors.totalTx, 
                           this.chartConfig.colors.totalTxTop, 
                           this.chartConfig.colors.totalTxRight);
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText('Total Network Transactions', legendX + 30, legendY + 7);
        
        // Address1 transactions legend (blue) - 3D style
        const address1LegendX = legendX + 280;
        this.draw3DLegendBar(ctx, address1LegendX, legendY, 20, 15, 3, 
                           this.chartConfig.colors.address1Tx, 
                           this.chartConfig.colors.address1TxTop, 
                           this.chartConfig.colors.address1TxRight);
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText(`${this.ADDRESS1.substring(0, 10)}... Transactions`, address1LegendX + 30, legendY + 7);
        
        // Agent transactions legend (grey) - only show if agent is configured
        if (this.AGENT) {
            const agentLegendX = address1LegendX + 280;
            this.draw3DLegendBar(ctx, agentLegendX, legendY, 20, 15, 3, 
                               this.chartConfig.colors.agentTx, 
                               this.chartConfig.colors.agentTxTop, 
                               this.chartConfig.colors.agentTxRight);
            ctx.fillStyle = this.chartConfig.colors.text;
            ctx.fillText(`${this.AGENT.substring(0, 10)}... (Agent) Transactions`, agentLegendX + 30, legendY + 7);
        }
        
        // Moving average lines legend
        const maLegendY = legendY - 20;
        
        // Dark blue line for address1 MA with 20% opacity
        ctx.strokeStyle = this.chartConfig.colors.movingAvgBlue;
        ctx.lineWidth = 4;
        ctx.lineCap = 'round';
        ctx.beginPath();
        ctx.moveTo(legendX, maLegendY + 7);
        ctx.lineTo(legendX + 20, maLegendY + 7);
        ctx.stroke();
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText('7-Day MA (Address)', legendX + 30, maLegendY + 7);
        
        // Dark orange line for total network MA with 20% opacity
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
        
        // Add summary statistics
        ctx.font = this.chartConfig.fonts.label;
        ctx.fillStyle = this.chartConfig.colors.text;
        ctx.fillText(`Address: ${this.ADDRESS1}`, legendX, legendY + 25);
        if (this.AGENT) {
            ctx.fillText(`Agent: ${this.AGENT}`, legendX + 400, legendY + 25);
        }
        ctx.fillText(`Blue percentages above bars represent share of total network transactions`, legendX, legendY - 35);

    }

    // Helper function to draw 3D legend bars
    draw3DLegendBar(ctx, x, y, width, height, depth, frontColor, topColor, rightColor) {
        // Draw front face
        ctx.fillStyle = frontColor;
        ctx.fillRect(x, y, width, height);
        
        // Draw top face (small parallelogram)
        ctx.fillStyle = topColor;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + depth, y - depth);
        ctx.lineTo(x + width + depth, y - depth);
        ctx.lineTo(x + width, y);
        ctx.closePath();
        ctx.fill();
        
        // Draw right face (small parallelogram)
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

// Main execution function
async function main() {
    try {
        const csvFilename = process.argv[2] || process.env.CSV_FILENAME;
        const outputFilename = process.argv[3];
        
        if (!csvFilename) {
            console.error(`[${new Date().toISOString()}] ERROR: ## Please provide CSV filename as argument or set CSV_FILENAME environment variable`);
            console.error(`[${new Date().toISOString()}] INFO: ## Usage: node analyzeChart.js <csv_filename> [output_filename.png]`);
            console.error(`[${new Date().toISOString()}] INFO: ## Example: node analyzeChart.js 0xfb8e879cb77aeb594850da75f30c7d777ce54513.csv chart.png`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set ADDRESS1 environment variable with the target address to track`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set AGENT environment variable with the agent address to track (optional)`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set TX_COUNT environment variable with transaction count data or API URL`);
            console.error(`[${new Date().toISOString()}] INFO: ## Set DEBUG=1 to enable debug logging, DEBUG=0 to disable (default)`);
            process.exit(1);
        }
        
        if (!process.env.ADDRESS1) {
            console.error(`[${new Date().toISOString()}] ERROR: ## ADDRESS1 environment variable is required`);
            process.exit(1);
        }
        
        try {
            fs.accessSync(csvFilename);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: ## CSV file not found: ${csvFilename}`);
            process.exit(1);
        }
        
        const chartGenerator = new TransactionChartGenerator();
        const startTime = Date.now();
        
        await chartGenerator.generateChart(csvFilename, outputFilename);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`[${new Date().toISOString()}] INFO: ## Chart generation completed in ${duration} seconds`);
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: ## Chart generation failed:`, error);
        process.exit(1);
    }
}

// Export the class for use in other modules
module.exports = { TransactionChartGenerator };

// Run main function if this file is executed directly
if (require.main === module) {
    main();
}