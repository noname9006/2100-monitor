# 2100-monitor

**Comprehensive Blockchain Analytics & Monitoring Toolkit**

---

## Overview

`2100-monitor` is a robust, feature-rich Node.js toolkit for advanced analytics and management of blockchain on-chain activity, focused on:
- Minting and transaction analytics (NFTs, tasks, tokens)
- Real-time and historical trend reporting
- Gamified blockchain/crypto-stat tracking
- Powerful, low-resource memory management for large datasets

## Features

- **Parallel On-Chain Analytics:**  
  Analyze transaction and minting events across multiple contracts in parallel. Built for efficiency on large historical datasets.

- **Powerful Chart Generation:**  
  Generates PNG charts with stacked area and combined daily mints, moving averages, enhanced weekly labeling, unique wallet tracking, and support for end-date visibility.

- **Transaction Scanning & Export:**  
  Track all transactions for a blockchain address or contract, with export to CSV and detailed breakdowns by time, type, and wallet.

- **Gaming/Token Counters:**  
  Included modules for "Satoshi Spin" leaderboards, STBTC token stats, and similar counters fetched from APIs or the blockchain.

- **Resource-Efficient Analysis:**  
  Integrated memory manager and ultra-low memory mode for processing on low-spec servers (e.g., Raspberry Pi or cloud VMs).

---

## Example Outputs

- **Cumulative task/NFT mints (stacked area chart)**
- **Combined daily mint chart (bars & trend line)**
- **Leaderboard/top wallet stats (e.g., Satoshi Spin)**
- **Complete CSV for all address activity**

All analytics are exportable for further reporting or sharing.

## Example Use Cases

- Tracking task/NFT/receipt mints in promotional campaigns
- Daily/weekly user participation stats for protocol or game
- Wallet analytics for on-chain campaigns
- Export activity stats for community/marketing

## Key Scripts & Modules

- `analyzeQuest.js` – Parallel analysis and chart generation for minting events, per contract/task (PNG output)
- `analyze.js`, `analyze_old.js` – Comprehensive transaction CSV analysis, categorized by wallet, type, time
- `2100Tx.js` – Transaction scanning and tracking for an address, with CSV history and deduplication
- `stbtc_counters.js`, `spin.js` – Game/task leaderboards and token counting from APIs

## Usage

1. **Install dependencies:**
   ```bash
   npm install
   ```
2. **Set up your `.env` file:**  
   Configure relevant environment variables (see samples below).

3. **Analyze a set of contracts (minting analytics):**
   ```bash
   node analyzeQuest.js
   ```
4. **Analyze transactions for a specific address:**
   ```bash
   node analyze.js <csv_file>
   ```

5. **Generate and view PNG charts:**  
   Output charts will be created in your working directory (requires Node.js Canvas and system libraries).

### Environment Variables (`.env` example)
```env
RPC_URL=https://your-rpc-url
START_BLOCK=1000000
TASK1=0xContractAddress1
TASK1_NAME=Quest #1
TASK2=0xContractAddress2
TASK2_NAME=Quest #2
# ...
CRON_SCHEDULE_COUNTER=*/5 * * * *
MAX_MEM_ANALYZE=2048
ULTRA_LOW_MEMORY=true
# Additional configuration for counters/APIs
```

## Advanced Features

- **Ultra-low memory mode for large-scale CSVs**
- **Unique wallet tracking**
- **Chunked, restartable blockchain scans**
- **Customizable chart colors, date ranges, legends**
- **API fetching for live counters/leaderboards**

## License

MIT

## Author

noname9006

---

For specific module details or help, refer to inline comments in each script and review sample output PNG/CSV files.