// Temporary stubs – replace with real MongoDB logic later
const connectdb = () => console.log('[DB] MongoDB connection stub – not actually connected');
const saveSessionToMongoDB = () => Promise.resolve();
const getSessionFromMongoDB = () => Promise.resolve(null); // null = no session
const deleteSessionFromMongoDB = () => Promise.resolve();
const getUserConfigFromMongoDB = () => Promise.resolve({}); // default empty config
const updateUserConfigInMongoDB = () => Promise.resolve();
const addNumberToMongoDB = () => Promise.resolve();
const removeNumberFromMongoDB = () => Promise.resolve();
const getAllNumbersFromMongoDB = () => Promise.resolve([]);
const saveOTPToMongoDB = () => Promise.resolve();
const verifyOTPFromMongoDB = () => Promise.resolve({ valid: false, error: 'No OTP stored' });
const incrementStats = () => Promise.resolve();
const getStatsForNumber = () => Promise.resolve({});

module.exports = {
    connectdb,
    saveSessionToMongoDB,
    getSessionFromMongoDB,
    deleteSessionFromMongoDB,
    getUserConfigFromMongoDB,
    updateUserConfigInMongoDB,
    addNumberToMongoDB,
    removeNumberFromMongoDB,
    getAllNumbersFromMongoDB,
    saveOTPToMongoDB,
    verifyOTPFromMongoDB,
    incrementStats,
    getStatsForNumber
};
