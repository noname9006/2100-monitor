const fs = require('fs');
const path = require('path');

// Function to parse log line and extract timestamp and totalSpins
function parseLogLine(line) {
    // Looking for lines like [2025-07-03T09:11:01.385Z] DEBUG: totalSpins=92205
    const match = line.match(/\[([^\]]+)\].*totalSpins=(\d+)/);
    if (match) {
        const timestamp = new Date(match[1]);
        const totalSpins = parseInt(match[2]);
        return { timestamp, totalSpins };
    }
    return null;
}

// Function to get date (without time) from timestamp
function getDateKey(timestamp) {
    return timestamp.toISOString().split('T')[0];
}

// Function to check if a day is completed (has data from 23:xx)
function isDayCompleted(entries) {
    return entries.some(entry => entry.timestamp.getUTCHours() === 23);
}

// Function to find midnight spins value for each date
function findMidnightSpins(logData) {
    const dailySpins = new Map();
    
    // Group data by dates
    logData.forEach(entry => {
        const dateKey = getDateKey(entry.timestamp);
        if (!dailySpins.has(dateKey)) {
            dailySpins.set(dateKey, []);
        }
        dailySpins.get(dateKey).push(entry);
    });
    
    // For each date find value closest to midnight
    const result = [];
    dailySpins.forEach((entries, date) => {
        // Only include completed days (with 23:xx data)
        if (!isDayCompleted(entries)) {
            return;
        }
        
        // Sort entries by time (closer to midnight = higher hours)
        entries.sort((a, b) => {
            const aHour = a.timestamp.getUTCHours();
            const bHour = b.timestamp.getUTCHours();
            
            // Priority: 23:xx > 22:xx > 21:xx > ... > 00:xx
            if (aHour === 23 && bHour !== 23) return -1;
            if (bHour === 23 && aHour !== 23) return 1;
            if (aHour === 22 && bHour !== 22) return -1;
            if (bHour === 22 && aHour !== 22) return 1;
            if (aHour === 21 && bHour !== 21) return -1;
            if (bHour === 21 && aHour !== 21) return 1;
            if (aHour === 0 && bHour !== 0) return -1;
            if (bHour === 0 && aHour !== 0) return 1;
            
            // If hours are the same, sort by minutes
            return a.timestamp.getUTCMinutes() - b.timestamp.getUTCMinutes();
        });
        
        // Take first value (closest to midnight)
        const midnightEntry = entries[0];
        result.push({
            date: date,
            totalSpins: midnightEntry.totalSpins,
            timestamp: midnightEntry.timestamp.toISOString()
        });
    });
    
    // Sort by date
    result.sort((a, b) => new Date(a.date) - new Date(b.date));
    
    // Calculate spins for each day
    // For correct calculation, we take value from previous day at 23:xx
    const dailySpinsResult = [];
    for (let i = 0; i < result.length; i++) {
        const current = result[i];
        let spinsForDay;
        
        if (i === 0) {
            // For first day take total amount
            spinsForDay = current.totalSpins;
        } else {
            // For other days calculate difference
            // current.totalSpins - value at end of current day
            // previous.totalSpins - value at end of previous day
            const previous = result[i - 1];
            spinsForDay = current.totalSpins - previous.totalSpins;
            
            // If difference is negative, counter was reset
            if (spinsForDay < 0) {
                spinsForDay = current.totalSpins;
            }
        }
        
        dailySpinsResult.push({
            date: current.date,
            spins: spinsForDay,
            totalSpins: current.totalSpins,
            timestamp: current.timestamp
        });
    }
    
    return dailySpinsResult;
}

// Function to calculate weighted average
function calculateWeightedAverage(data) {
    if (data.length === 0) return 0;
    
    let totalSpins = 0;
    let totalWeight = 0;
    
    data.forEach((entry, index) => {
        const weight = index + 1; // More recent days have higher weight
        totalSpins += entry.spins * weight;
        totalWeight += weight;
    });
    
    return totalSpins / totalWeight;
}

// Function to export to CSV
function exportToCSV(data, filename) {
    const csvHeader = 'Date,Spins\n';
    const csvContent = data.map(row => `${row.date},${row.spins}`).join('\n');
    const fullContent = csvHeader + csvContent;
    
    fs.writeFileSync(filename, fullContent, 'utf8');
    console.log(`Data exported to file: ${filename}`);
}

// Main function
function main() {
    const logFile = 'index-out.log';
    const outputFile = 'spins_daily.csv';
    
    console.log('Starting log file analysis...');
    
    // Check if file exists
    if (!fs.existsSync(logFile)) {
        console.error(`Error: File ${logFile} not found!`);
        process.exit(1);
    }
    
    try {
        // Read file line by line
        const logContent = fs.readFileSync(logFile, 'utf8');
        const lines = logContent.split('\n');
        
        console.log(`Processing ${lines.length} lines...`);
        
        // Parse lines and extract data
        const logData = [];
        let processedLines = 0;
        
        lines.forEach((line, index) => {
            if (index % 10000 === 0) {
                console.log(`Processed lines: ${index}/${lines.length}`);
            }
            
            const parsed = parseLogLine(line);
            if (parsed) {
                logData.push(parsed);
            }
            processedLines++;
        });
        
        console.log(`Found ${logData.length} totalSpins entries`);
        
        if (logData.length === 0) {
            console.log('No totalSpins entries found. Check log format.');
            return;
        }
        
        // Analyze data by days
        console.log('Analyzing data by days...');
        const dailyData = findMidnightSpins(logData);
        
        console.log(`Found data for ${dailyData.length} completed days`);
        
        if (dailyData.length === 0) {
            console.log('No completed days found.');
            return;
        }
        
        // Show first few entries for verification
        console.log('\nFirst entries:');
        dailyData.slice(0, 5).forEach(entry => {
            console.log(`${entry.date}: ${entry.spins.toLocaleString()} spins per day (total: ${entry.totalSpins.toLocaleString()})`);
        });
        
        // Calculate statistics
        const allTimeAverage = dailyData.reduce((sum, entry) => sum + entry.spins, 0) / dailyData.length;
        const last7Days = dailyData.slice(-7);
        const last7DaysAverage = last7Days.reduce((sum, entry) => sum + entry.spins, 0) / last7Days.length;
        const weightedAverage = calculateWeightedAverage(dailyData);
        const last7DaysWeighted = calculateWeightedAverage(last7Days);
        
        console.log('\n=== STATISTICS ===');
        console.log(`All time average: ${allTimeAverage.toLocaleString(undefined, {maximumFractionDigits: 0})} spins per day`);
        console.log(`Last 7 days average: ${last7DaysAverage.toLocaleString(undefined, {maximumFractionDigits: 0})} spins per day`);
        console.log(`All time weighted average: ${weightedAverage.toLocaleString(undefined, {maximumFractionDigits: 0})} spins per day`);
        console.log(`Last 7 days weighted average: ${last7DaysWeighted.toLocaleString(undefined, {maximumFractionDigits: 0})} spins per day`);
        
        // Show last 7 days details
        if (last7Days.length > 0) {
            console.log('\nLast 7 days details:');
            last7Days.forEach(entry => {
                console.log(`${entry.date}: ${entry.spins.toLocaleString()} spins`);
            });
        }
        
        // Export to CSV
        exportToCSV(dailyData, outputFile);
        
        console.log('\nAnalysis completed successfully!');
        
    } catch (error) {
        console.error('Error processing file:', error.message);
        process.exit(1);
    }
}

// Run script
if (require.main === module) {
    main();
}

module.exports = { parseLogLine, findMidnightSpins, exportToCSV, calculateWeightedAverage };
