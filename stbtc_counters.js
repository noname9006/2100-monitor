const axios = require('axios');
const cron = require('node-cron');

class StbtcCounters {
    constructor(client) {
        this.client = client;
        this.isRunning = false;
    }

    /**
     * Start the STBTC counters with cron scheduling
     */
    start() {
        const cronSchedule = process.env.CRON_SCHEDULE_COUNTER;
        if (!cronSchedule) {
            console.warn(`[${new Date().toISOString()}] WARN: CRON_SCHEDULE_COUNTER not set in .env`);
            return;
        }

        if (this.isRunning) {
            console.warn(`[${new Date().toISOString()}] WARN: STBTC counters already running`);
            return;
        }

        console.log(`[${new Date().toISOString()}] INFO: Starting STBTC counters with schedule: ${cronSchedule}`);
        
        cron.schedule(cronSchedule, async () => {
            await this.updateAllCounters();
        });

        this.isRunning = true;
    }

    /**
     * Update all STBTC counters
     */
    async updateAllCounters() {
        console.log(`[${new Date().toISOString()}] INFO: Updating STBTC counters...`);
        
        await Promise.all([
            this.updateTokenQuantityCounter(),
            this.updateHoldersCounter(),
            this.updateExchangeRateCounter(),
            this.updateCapPercentageCounter()
        ]);
    }

    /**
     * Counter 1: Token quantity from ERC20 holdings
     */
    async updateTokenQuantityCounter() {
        try {
            const channelId = process.env.STBTC1_CHANNELID;
            const nameTemplate = process.env.STBTC1_CHANNELNAME;
            
            if (!channelId || !nameTemplate) {
                console.warn(`[${new Date().toISOString()}] WARN: STBTC1_CHANNELID or STBTC1_CHANNELNAME not set in .env`);
                return;
            }

            const response = await axios.get('https://api.routescan.io/v2/network/mainnet/evm/3637/address/0xF4586028FFdA7Eca636864F80f8a3f2589E33795/erc20-holdings');
            
            const targetToken = response.data.items?.find(token => 
                token.tokenAddress === '0x0D2437F93Fed6EA64Ef01cCde385FB1263910C56'
            );

            if (!targetToken) {
                console.error(`[${new Date().toISOString()}] ERROR: Target token not found in holdings`);
                return;
            }

            const tokenQuantity = parseFloat(targetToken.tokenQuantity);
            const tokenDecimals = parseInt(targetToken.tokenDecimals || 18);
            const stbtcDecimals = parseInt(process.env.STBTC1_DECIMALS || 2);

            // Calculate actual value considering token decimals
            const actualValue = tokenQuantity / Math.pow(10, tokenDecimals);
            
            // Round to specified decimals and format with fixed decimal places
            const roundedValue = Math.round(actualValue * Math.pow(10, stbtcDecimals)) / Math.pow(10, stbtcDecimals);
            const displayValue = roundedValue.toFixed(stbtcDecimals);

            await this.updateChannelName(channelId, nameTemplate, displayValue);
            
            console.log(`[${new Date().toISOString()}] INFO: Updated STBTC token quantity counter: ${displayValue}`);
            
            // Store for cap percentage calculation (as number for calculations)
            this.lastTokenQuantity = roundedValue;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: Failed to update token quantity counter:`, error.message);
        }
    }

    /**
     * Counter 2: Token holders count
     */
    async updateHoldersCounter() {
        try {
            const channelId = process.env.STBTC2_CHANNELID;
            const nameTemplate = process.env.STBTC2_CHANNELNAME;
            
            if (!channelId || !nameTemplate) {
                console.warn(`[${new Date().toISOString()}] WARN: STBTC2_CHANNELID or STBTC2_CHANNELNAME not set in .env`);
                return;
            }

            const response = await axios.get('https://api.routescan.io/v2/network/mainnet/evm/3637/erc20/0xF4586028FFdA7Eca636864F80f8a3f2589E33795/holders?count=true');
            
            const holdersCount = response.data.count || 0;

            await this.updateChannelName(channelId, nameTemplate, holdersCount);
            
            console.log(`[${new Date().toISOString()}] INFO: Updated STBTC holders counter: ${holdersCount}`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: Failed to update holders counter:`, error.message);
        }
    }

    /**
     * Counter 3: Exchange rate from most recent date
     */
    async updateExchangeRateCounter() {
        try {
            const channelId = process.env.STBTC3_CHANNELID;
            const nameTemplate = process.env.STBTC3_CHANNELNAME;
            
            if (!channelId || !nameTemplate) {
                console.warn(`[${new Date().toISOString()}] WARN: STBTC3_CHANNELID or STBTC3_CHANNELNAME not set in .env`);
                return;
            }

            const response = await axios.get('https://sidecar.botanixlabs.com/api/exchangeRate');
            
            // Check for response.data.rates instead of response.data
            if (!response.data || !response.data.rates || !Array.isArray(response.data.rates) || response.data.rates.length === 0) {
                console.error(`[${new Date().toISOString()}] ERROR: Invalid exchange rate data`);
                return;
            }

            // Find the most recent date from the rates array
            const sortedData = response.data.rates.sort((a, b) => new Date(b.date) - new Date(a.date));
            const mostRecent = sortedData[0];
            
            // Get decimals from env
            const stbtcDecimals = parseInt(process.env.STBTC3_DECIMALS || 6);
            
            // Get exchange rate as number (not percentage)
            const exchangeRate = parseFloat(mostRecent.rate || mostRecent.exchangeRate || 0);
            const roundedValue = Math.round(exchangeRate * Math.pow(10, stbtcDecimals)) / Math.pow(10, stbtcDecimals);
            const displayValue = roundedValue.toFixed(stbtcDecimals);

            await this.updateChannelName(channelId, nameTemplate, displayValue);
            
            console.log(`[${new Date().toISOString()}] INFO: Updated STBTC exchange rate counter: ${displayValue}`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: Failed to update exchange rate counter:`, error.message);
        }
    }

    /**
     * Counter 4: Cap percentage (token quantity / STBTC_CAP)
     */
    async updateCapPercentageCounter() {
        try {
            const channelId = process.env.STBTC4_CHANNELID;
            const nameTemplate = process.env.STBTC4_CHANNELNAME;
            const stbtcCap = parseFloat(process.env.STBTC_CAP);
            
            if (!channelId || !nameTemplate) {
                console.warn(`[${new Date().toISOString()}] WARN: STBTC4_CHANNELID or STBTC4_CHANNELNAME not set in .env`);
                return;
            }

            if (!stbtcCap || isNaN(stbtcCap)) {
                console.warn(`[${new Date().toISOString()}] WARN: STBTC_CAP not set or invalid in .env`);
                return;
            }

            // Use stored token quantity from counter 1, or fetch it if not available
            let tokenQuantity = this.lastTokenQuantity;
            
            if (!tokenQuantity) {
                console.log(`[${new Date().toISOString()}] DEBUG: Token quantity not available, fetching...`);
                await this.updateTokenQuantityCounter();
                tokenQuantity = this.lastTokenQuantity;
            }

            if (!tokenQuantity || tokenQuantity === 0) {
                console.error(`[${new Date().toISOString()}] ERROR: Cannot calculate cap percentage - token quantity is 0 or unavailable`);
                return;
            }

            // Get decimals from env
            const stbtcDecimals = parseInt(process.env.STBTC4_DECIMALS || 2);

            // Calculate percentage: token_quantity / STBTC_CAP * 100
            const percentage = (tokenQuantity / stbtcCap) * 100;
            const roundedValue = Math.round(percentage * Math.pow(10, stbtcDecimals)) / Math.pow(10, stbtcDecimals);
            const displayValue = roundedValue.toFixed(stbtcDecimals);

            await this.updateChannelName(channelId, nameTemplate, `${displayValue}%`);
            
            console.log(`[${new Date().toISOString()}] INFO: Updated STBTC cap percentage counter: ${displayValue}% (${tokenQuantity}/${stbtcCap})`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: Failed to update cap percentage counter:`, error.message);
        }
    }

    /**
     * Helper method to update Discord channel name
     */
    async updateChannelName(channelId, nameTemplate, value) {
        try {
            if (!this.client) {
                console.error(`[${new Date().toISOString()}] ERROR: Discord client is required for updating channel names.`);
                return;
            }

            const channel = await this.client.channels.fetch(channelId);
            
            if (!channel) {
                console.warn(`[${new Date().toISOString()}] WARN: Channel not found for ID: ${channelId}`);
                return;
            }

            // Replace placeholders in the template
            let newName = nameTemplate.replace('{count}', value);
            
            // Replace {STBTC_CAP} with the environment variable value
            if (newName.includes('{STBTC_CAP}')) {
                const stbtcCap = process.env.STBTC_CAP || '0';
                newName = newName.replace('{STBTC_CAP}', stbtcCap);
            }
            
            await channel.setName(newName);
            
            console.log(`[${new Date().toISOString()}] DEBUG: Updated channel ${channelId} name to: ${newName}`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ERROR: Failed to update channel name for ${channelId}:`, error.message);
        }
    }

    /**
     * Stop the cron job
     */
    stop() {
        if (this.isRunning) {
            cron.destroy();
            this.isRunning = false;
            console.log(`[${new Date().toISOString()}] INFO: STBTC counters stopped`);
        }
    }

    /**
     * Manual update for testing
     */
    async manualUpdate() {
        console.log(`[${new Date().toISOString()}] INFO: Manual STBTC counters update triggered`);
        await this.updateAllCounters();
    }
}

module.exports = {
    StbtcCounters
};