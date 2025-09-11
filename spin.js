const fetch = require('node-fetch');

const ROUTESCAN_API = 'https://api.routescan.io/v2/network/mainnet/evm/3637/address/0xFB8e879Cb77AEB594850DA75F30C7d777ce54513/transactions?direction=received&count=true&limit=1';
const LEADERBOARD_API = 'https://2100abitcoinworld.com/api/wheel/leaderboard?leaderboardType=global';

async function getTotalSpins() {
    try {
        const res = await fetch(ROUTESCAN_API);
        const data = await res.json();
        console.log(`[${new Date().toISOString()}] DEBUG: RouteScan API response:`, data);
        return data.count || 0;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Error fetching total spins:`, error);
        return 0;
    }
}

async function getLeaderboard() {
    try {
        const res = await fetch(LEADERBOARD_API);
        const data = await res.json();
        return data.slice(0, 10);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ERROR: Error fetching leaderboard:`, error);
        return [];
    }
}

function formatAddress(address) {
    if (typeof address !== 'string' || address.length < 10) return address;
    return `${address.substring(0, 4)}...${address.substring(address.length - 4)}`;
}

function getEmoji(index) {
    const emojis = ['🥇  ', '🥈  ', '🥉  '];
    if (index < 3) {
        return emojis[index];
    }
    return '🔸   ';
}

async function createSatoshiSpinEmbed() {
    const totalSpins = await getTotalSpins();
    const leaderboard = await getLeaderboard();

    const winnersList = leaderboard.map((player, index) => {
        return `${getEmoji(index)} \`${formatAddress(player.value)}\``;
    }).join('\n');

    const scoresList = leaderboard.map(player => {
        return `\`  ${player.score}\``;
    }).join('\n');

    return {
        embeds: [{
            title: '🚨    Satoshi Spin    🚨',
            description: `\u200B\n🔄  Total spins: **${totalSpins}**\n\u200B`,
            color: 0xFFD700,
            fields: [
                {
                    name: ' Top 10 Winners 💎',
                    value: '\u200B\n' + winnersList || 'N/A',
                    inline: true
                },
                {
                    name: 'Sats Won 💰',
                    value: '\u200B\n' + scoresList || 'N/A',
                    inline: true
                }
            ]
        }]
    };
}

module.exports = {
    createSatoshiSpinEmbed,
    getTotalSpins
};

process.on('unhandledRejection', (error) => {
    console.error(`[${new Date().toISOString()}] ERROR: ❌ Unhandled promise rejection:`, error);
}); 