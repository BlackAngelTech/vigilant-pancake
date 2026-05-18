const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    makeCacheableSignalKeyStore,
    jidNormalizedUser,
    Browsers,
    DisconnectReason,
    jidDecode,
    generateForwardMessageContent,
    generateWAMessageFromContent,
    downloadContentFromMessage,
    getContentType,
    makeInMemoryStore
} = require('@whiskeysockets/baileys');

const config = require('./config');
const events = require('./inconnuboy');
const { sms } = require('./lib/msg');
const { 
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
} = require('./lib/database');
const { handleAntidelete } = require('./lib/antidelete');

const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const pino = require('pino');
const crypto = require('crypto');
const FileType = require('file-type');
const axios = require('axios');
const bodyparser = require('body-parser');
const moment = require('moment-timezone');

const prefix = config.PREFIX;
const mode = config.MODE;
const router = express.Router();

// ==============================================================================
// 1. INITIALIZATION & DATABASE
// ==============================================================================

connectdb();

// Stockage en mémoire
const activeSockets = new Map();
const socketCreationTime = new Map();

// Store pour anti-delete et messages
const store = makeInMemoryStore({ 
    logger: pino().child({ level: 'silent', stream: 'store' }) 
});

// Fonctions utilitaires
const createSerial = (size) => {
    return crypto.randomBytes(size).toString('hex').slice(0, size);
}

const getGroupAdmins = (participants) => {
    let admins = [];
    for (let i of participants) {
        if (i.admin == null) continue;
        admins.push(i.id);
    }
    return admins;
}

// Vérification connexion existante
function isNumberAlreadyConnected(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    return activeSockets.has(sanitizedNumber);
}

function getConnectionStatus(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const isConnected = activeSockets.has(sanitizedNumber);
    const connectionTime = socketCreationTime.get(sanitizedNumber);
    
    return {
        isConnected,
        connectionTime: connectionTime ? new Date(connectionTime).toLocaleString() : null,
        uptime: connectionTime ? Math.floor((Date.now() - connectionTime) / 1000) : 0
    };
}

// Fonction de log personnalisée
function inconnuLog(message, type = 'info') {
    const timestamp = new Date().toISOString();
    const types = {
        info: '📝',
        success: '✅',
        error: '❌',
        warning: '⚠️',
        debug: '🐛'
    };
    
    const emoji = types[type] || '📝';
    console.log(`${emoji} [INCONNU-BOY] ${timestamp}: ${message}`);
}

// Load Plugins
const pluginsDir = path.join(__dirname, 'plugins');
if (!fs.existsSync(pluginsDir)) {
    fs.mkdirSync(pluginsDir, { recursive: true });
}

const files = fs.readdirSync(pluginsDir).filter(file => file.endsWith('.js'));
inconnuLog(`Loading ${files.length} plugins...`, 'info');
for (const file of files) {
    try {
        require(path.join(pluginsDir, file));
    } catch (e) {
        inconnuLog(`Failed to load plugin ${file}: ${e.message}`, 'error');
    }
}

// ==============================================================================
// 2. HANDLERS SPÉCIFIQUES
// ==============================================================================

async function setupMessageHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        // Charger config utilisateur depuis MongoDB
        const userConfig = await getUserConfigFromMongoDB(number);
        
        // Auto-typing basé sur config
        if (userConfig.AUTO_TYPING === 'true') {
            try {
                await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
            } catch (error) {
                inconnuLog(`Failed to set typing presence: ${error.message}`, 'error');
            }
        }
        
        // Auto-recording basé sur config
        if (userConfig.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
            } catch (error) {
                inconnuLog(`Failed to set recording presence: ${error.message}`, 'error');
            }
        }
    });
}

async function setupCallHandlers(socket, number) {
    socket.ev.on('call', async (calls) => {
        try {
            // Charger config utilisateur depuis MongoDB
            const userConfig = await getUserConfigFromMongoDB(number);
            if (userConfig.ANTI_CALL !== 'true') return;

            for (const call of calls) {
                if (call.status !== 'offer') continue;
                const id = call.id;
                const from = call.from;

                await socket.rejectCall(id, from);
                await socket.sendMessage(from, {
                    text: userConfig.REJECT_MSG || '*🔕 ʏᴏᴜʀ ᴄᴀʟʟ ᴡᴀs ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ʀᴇᴊᴇᴄᴛᴇᴅ..!*'
                });
                inconnuLog(`Auto-rejected call for user ${number} from ${from}`, 'info');
            }
        } catch (err) {
            inconnuLog(`Anti-call error for ${number}: ${err.message}`, 'error');
        }
    });
}

function setupAutoRestart(socket, number) {
    let restartAttempts = 0;
    const maxRestartAttempts = 3;
    
    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        
        inconnuLog(`Connection update for ${number}: ${JSON.stringify({ connection, lastDisconnect: lastDisconnect?.error?.message })}`, 'debug');
        
        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const errorMessage = lastDisconnect?.error?.message;
            
            inconnuLog(`Connection closed for ${number}: ${statusCode} - ${errorMessage}`, 'warning');
            
            // Manual unlink detection
            if (statusCode === 401 || errorMessage?.includes('401')) {
                inconnuLog(`Manual unlink detected for ${number}, cleaning up...`, 'warning');
                const sanitizedNumber = number.replace(/[^0-9]/g, '');
                
                // IMPORTANT: Supprimer la session, le numéro actif et le socket
                activeSockets.delete(sanitizedNumber);
                socketCreationTime.delete(sanitizedNumber);
                await deleteSessionFromMongoDB(sanitizedNumber);
                await removeNumberFromMongoDB(sanitizedNumber);
                
                // Arrêter l'écoute des événements sur ce socket
                socket.ev.removeAllListeners();
                return;
            }
            
            // Skip restart for normal/expected errors
            const isNormalError = statusCode === 408 || 
                                errorMessage?.includes('QR refs attempts ended');
            
            if (isNormalError) {
                inconnuLog(`Normal connection closure for ${number} (${errorMessage}), no restart needed.`, 'info');
                return;
            }
            
            // For other unexpected errors, attempt reconnect with limits
            if (restartAttempts < maxRestartAttempts) {
                restartAttempts++;
                inconnuLog(`Unexpected connection lost for ${number}, attempting to reconnect (${restartAttempts}/${maxRestartAttempts}) in 10 seconds...`, 'warning');
                
                // Supprimer de activeSockets avant de tenter le reconnect
                const sanitizedNumber = number.replace(/[^0-9]/g, '');
                activeSockets.delete(sanitizedNumber);
                socketCreationTime.delete(sanitizedNumber);
                
                // Supprimer les listeners de l'ancien socket pour éviter les fuites de mémoire
                socket.ev.removeAllListeners();

                // Wait and reconnect
                await delay(10000);
                
                try {
                    const mockRes = { 
                        headersSent: false, 
                        send: () => {}, 
                        status: () => mockRes,
                        setHeader: () => {},
                        json: () => {}
                    };
                    // Tenter de redémarrer le bot, qui va charger la session MongoDB
                    await inconnuboyPair(number, mockRes);
                    inconnuLog(`Reconnection initiated for ${number}`, 'success');
                } catch (reconnectError) {
                    inconnuLog(`Reconnection failed for ${number}: ${reconnectError.message}`, 'error');
                }
            } else {
                inconnuLog(`Max restart attempts reached for ${number}. Manual intervention required.`, 'error');
            }
        }
        
        // Reset counter on successful connection
        if (connection === 'open') {
            inconnuLog(`Connection established for ${number}`, 'success');
            restartAttempts = 0;
        }
    });
}

// ==============================================================================
// 3. FONCTION PRINCIPALE INCONNUBOYPAIR
// ==============================================================================

async function inconnuboyPair(number, res = null) {
    let connectionLockKey;
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    
    try {
        const sessionDir = path.join(__dirname, 'session', `session_${sanitizedNumber}`);
        
        // Vérifier si déjà connecté
        if (isNumberAlreadyConnected(sanitizedNumber)) {
            inconnuLog(`${sanitizedNumber} is already connected, skipping...`, 'info');
            const status = getConnectionStatus(sanitizedNumber);
            
            if (res && !res.headersSent) {
                return res.json({ 
                    status: 'already_connected', 
                    message: 'Number is already connected and active',
                    connectionTime: status.connectionTime,
                    uptime: `${status.uptime} seconds`
                });
            }
            return;
        }
        
        // Verrou pour éviter connexions simultanées
        connectionLockKey = `connecting_${sanitizedNumber}`;
        if (global[connectionLockKey]) {
            inconnuLog(`${sanitizedNumber} is already in connection process, skipping...`, 'info');
            if (res && !res.headersSent) {
                return res.json({ 
                    status: 'connection_in_progress', 
                    message: 'Number is currently being connected'
                });
            }
            return;
        }
        global[connectionLockKey] = true;
        
        // 1. Vérifier session MongoDB
        const existingSession = await getSessionFromMongoDB(sanitizedNumber);
        
        if (!existingSession) {
            inconnuLog(`No MongoDB session found for ${sanitizedNumber} - requiring NEW pairing`, 'info');
            
            // Nettoyer fichiers locaux
            if (fs.existsSync(sessionDir)) {
                await fs.remove(sessionDir);
                inconnuLog(`Cleaned leftover local session for ${sanitizedNumber}`, 'info');
            }
        } else {
            // Restaurer depuis MongoDB
            fs.ensureDirSync(sessionDir);
            fs.writeFileSync(path.join(sessionDir, 'creds.json'), JSON.stringify(existingSession, null, 2));
            inconnuLog(`Restored existing session from MongoDB for ${sanitizedNumber}`, 'success');
        }
        
        // 2. Initialiser socket
        const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
        
        const conn = makeWASocket({
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }))
            },
            printQRInTerminal: false,
            usePairingCode: !existingSession, 
            logger: pino({ level: 'silent' }),
            browser: Browsers.macOS('Safari'),
            syncFullHistory: false,
            getMessage: async (key) => {
                if (store) {
                    const msg = await store.loadMessage(key.remoteJid, key.id);
                    return msg?.message || undefined;
                }
                return { conversation: 'Hello' };
            }
        });
        
        // 3. Enregistrer connexion
        socketCreationTime.set(sanitizedNumber, Date.now());
        activeSockets.set(sanitizedNumber, conn);
        store.bind(conn.ev);
        
        // 4. Setup handlers
        setupMessageHandlers(conn, number);
        setupCallHandlers(conn, number);
        setupAutoRestart(conn, number);
        
        // 5. UTILS ATTACHED TO CONN
        conn.decodeJid = jid => {
            if (!jid) return jid;
            if (/:\d+@/gi.test(jid)) {
                let decode = jidDecode(jid) || {};
                return (decode.user && decode.server && decode.user + '@' + decode.server) || jid;
            } else return jid;
        };
        
        conn.downloadAndSaveMediaMessage = async(message, filename, attachExtension = true) => {
            let quoted = message.msg ? message.msg : message;
            let mime = (message.msg || message).mimetype || '';
            let messageType = message.mtype ? message.mtype.replace(/Message/gi, '') : mime.split('/')[0];
            const stream = await downloadContentFromMessage(quoted, messageType);
            let buffer = Buffer.from([]);
            for await (const chunk of stream) {
                buffer = Buffer.concat([buffer, chunk]);
            }
            let type = await FileType.fromBuffer(buffer);
            let trueFileName = attachExtension ? (filename + '.' + type.ext) : filename;
            await fs.writeFileSync(trueFileName, buffer);
            return trueFileName;
        };
        
        // 6. PAIRING CODE GENERATION
        if (!existingSession) {
            setTimeout(async () => {
                try {
                    await delay(1500);
                    const code = await conn.requestPairingCode(sanitizedNumber);
                    inconnuLog(`Pairing Code: ${code}`, 'success');
                    if (res && !res.headersSent) {
                        return res.json({ 
                            code: code, 
                            status: 'new_pairing',
                            message: 'New pairing required'
                        });
                    }
                } catch (err) {
                    inconnuLog(`Pairing Error: ${err.message}`, 'error');
                    if (res && !res.headersSent) {
                        return res.json({ 
                            error: 'Failed to generate pairing code',
                            details: err.message 
                        });
                    }
                }
            }, 3000);
        } else if (res && !res.headersSent) {
            res.json({
                status: 'reconnecting',
                message: 'Attempting to reconnect with existing session data'
            });
        }
        
        // 7. Sauvegarde session dans MongoDB
        conn.ev.on('creds.update', async () => {
            await saveCreds();
            const fileContent = fs.readFileSync(path.join(sessionDir, 'creds.json'), 'utf8');
            const creds = JSON.parse(fileContent);
            
            await saveSessionToMongoDB(sanitizedNumber, creds);
            inconnuLog(`Session updated in MongoDB for ${sanitizedNumber}`, 'success');
        });
        
        // 8. GESTION CONNEXION
        conn.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            
            if (connection === 'open') {
                inconnuLog(`Connected: ${sanitizedNumber}`, 'success');
                const userJid = jidNormalizedUser(conn.user.id);
                
                // Ajouter aux numéros actifs
                await addNumberToMongoDB(sanitizedNumber);
                
                // Message de bienvenue
                const connectText = `
               ╭────────────────────◇*
│•* *➺ ᴄᴏɴɴᴇᴄᴛᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ ᴛʏᴘᴇ*
*│•* *${prefix}ᴍᴇɴᴜ ᴛᴏ sᴇᴇ ᴛʜᴇ ғᴜʟʟ ᴄᴏᴍᴍᴀɴᴅ ʟɪsᴛ💫*
*│•* *ᴊᴏɪɴ ᴏᴜʀ ᴡʜᴀᴛsᴀᴘᴘ ᴄʜᴀɴɴᴇʟ ғᴏʀ ᴜᴘᴅᴀᴛᴇs ʙᴏᴛ*
*│•* ➳ ᴘʀᴇғɪx 『 ${prefix} 』
*│•* ➳ ᴍᴏᴅᴇ 〔〔${mode}〕〕
*╰────────────────────○*
*ᴘᴏᴡᴇʀᴇᴅ ʙʏ inconnu boy*`;
                
                if (!existingSession) {
                    await conn.sendMessage(userJid, {
                        image: { url: config.IMAGE_PATH },
                        caption: connectText
                    });
                }
                
                inconnuLog(`${sanitizedNumber} successfully connected!`, 'success');
            }
            
            if (connection === 'close') {
                let reason = lastDisconnect?.error?.output?.statusCode;
                if (reason === DisconnectReason.loggedOut) {
                    inconnuLog(`Session closed: Logged Out.`, 'error');
                }
            }
        });
        
        // 9. ANTI-CALL avec config MongoDB
        conn.ev.on('call', async (calls) => {
            try {
                const userConfig = await getUserConfigFromMongoDB(number);
                if (userConfig.ANTI_CALL !== 'true') return;
                
                for (const call of calls) {
                    if (call.status !== 'offer') continue;
                    const id = call.id;
                    const from = call.from;
                    await conn.rejectCall(id, from);
                    await conn.sendMessage(from, { 
                        text: userConfig.REJECT_MSG || config.REJECT_MSG 
                    });
                }
            } catch (err) { 
                inconnuLog(`Anti-call error: ${err.message}`, 'error');
            }
        });
        
        // 10. ANTIDELETE
        conn.ev.on('messages.update', async (updates) => {
            await handleAntidelete(conn, updates, store);
        });
        
        // ===============================================================
        // 📥 MESSAGE HANDLER (UPSERT) AVEC CONFIG MONGODB
        // ===============================================================
        conn.ev.on('messages.upsert', async (msg) => {
            try {
                let mek = msg.messages[0];
                if (!mek.message) return;
                
                // Charger config utilisateur
                const userConfig = await getUserConfigFromMongoDB(number);
                
                // Normalize Message
                mek.message = (getContentType(mek.message) === 'ephemeralMessage') 
                    ? mek.message.ephemeralMessage.message 
                    : mek.message;
                
                if (mek.message.viewOnceMessageV2) {
                    mek.message = (getContentType(mek.message) === 'ephemeralMessage') 
                        ? mek.message.ephemeralMessage.message 
                        : mek.message;
                }
                
                // Auto Read basé sur config
                if (userConfig.READ_MESSAGE === 'true') {
                    await conn.readMessages([mek.key]);
                }
                
                // Newsletter Reaction
                const newsletterJids = ["120363403408693274@newsletter"];
                const newsEmojis = ["❤️", "👍", "😮", "😎", "💀", "💫", "🔥", "👑"];
                if (mek.key && newsletterJids.includes(mek.key.remoteJid)) {
                    try {
                        const serverId = mek.newsletterServerId;
                        if (serverId) {
                            const emoji = newsEmojis[Math.floor(Math.random() * newsEmojis.length)];
                            await conn.newsletterReactMessage(mek.key.remoteJid, serverId.toString(), emoji);
                        }
                    } catch (e) {}
                }
                
                // Status Handling avec config MongoDB
                if (mek.key && mek.key.remoteJid === 'status@broadcast') {
                    // Auto View
                    if (userConfig.AUTO_VIEW_STATUS === "true") await conn.readMessages([mek.key]);
                    
                    // Auto Like
                    if (userConfig.AUTO_LIKE_STATUS === "true") {
                        const jawadlike = await conn.decodeJid(conn.user.id);
                        const emojis = userConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
                        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                        await conn.sendMessage(mek.key.remoteJid, {
                            react: { text: randomEmoji, key: mek.key } 
                        }, { statusJidList: [mek.key.participant, jawadlike] });
                    }
                    
                    // Auto Reply
                    if (userConfig.AUTO_STATUS_REPLY === "true") {
                        const user = mek.key.participant;
                        const text = userConfig.AUTO_STATUS_MSG || config.AUTO_STATUS_MSG;
                        await conn.sendMessage(user, { 
                            text: text, 
                            react: { text: '💫', key: mek.key } 
                        }, { quoted: mek });
                    }
                    return; 
                }
                
                // Message Serialization
                const m = sms(conn, mek);
                const type = getContentType(mek.message);
                const from = mek.key.remoteJid;
                const quoted = type == 'extendedTextMessage' && mek.message.extendedTextMessage.contextInfo != null ? mek.message.extendedTextMessage.contextInfo.quotedMessage || [] : [];
                const body = (type === 'conversation') ? mek.message.conversation : (type === 'extendedTextMessage') ? mek.message.extendedTextMessage.text : '';
                
                const isCmd = body.startsWith(config.PREFIX);
                const command = isCmd ? body.slice(config.PREFIX.length).trim().split(' ').shift().toLowerCase() : '';
                const args = body.trim().split(/ +/).slice(1);
                const q = args.join(' ');
                const text = q;
                const isGroup = from.endsWith('@g.us');
                
                const sender = mek.key.fromMe ? (conn.user.id.split(':')[0]+'@s.whatsapp.net' || conn.user.id) : (mek.key.participant || mek.key.remoteJid);
                const senderNumber = sender.split('@')[0];
                const botNumber = conn.user.id.split(':')[0];
                const botNumber2 = await jidNormalizedUser(conn.user.id);
                const pushname = mek.pushName || 'User';
                
                const isMe = botNumber.includes(senderNumber);
                const isOwner = config.OWNER_NUMBER.includes(senderNumber) || isMe;
                const isCreator = isOwner;
                
                // Group Metadata
                let groupMetadata = null;
                let groupName = null;
                let participants = null;
                let groupAdmins = null;
                let isBotAdmins = null;
                let isAdmins = null;
                
                if (isGroup) {
                    try {
                        groupMetadata = await conn.groupMetadata(from);
                        groupName = groupMetadata.subject;
                        participants = await groupMetadata.participants;
                        groupAdmins = await getGroupAdmins(participants);
                        isBotAdmins = groupAdmins.includes(botNumber2);
                        isAdmins = groupAdmins.includes(sender);
                    } catch(e) {}
                }
                
                // Auto Presence basé sur config MongoDB
                if (userConfig.AUTO_TYPING === 'true') await conn.sendPresenceUpdate('composing', from);
                if (userConfig.AUTO_RECORDING === 'true') await conn.sendPresenceUpdate('recording', from);
                
                // Custom MyQuoted
                const myquoted = {
                    key: {
                        remoteJid: 'status@broadcast',
                        participant: '13135550002@s.whatsapp.net',
                        fromMe: false,
                        id: createSerial(16).toUpperCase()
                    },
                    message: {
                        contactMessage: {
                            displayName: "© Inconnu boy",
                            vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:Inconnu boy\nORG:Inconnu boy;\nTEL;type=CELL;type=VOICE;waid=13135550002:13135550002\nEND:VCARD`,
                            contextInfo: {
                                stanzaId: createSerial(16).toUpperCase(),
                                participant: "0@s.whatsapp.net",
                                quotedMessage: { conversation: "© Inconnu boy" }
                            }
                        }
                    },
                    messageTimestamp: Math.floor(Date.now() / 1000),
                    status: 1,
                    verifiedBizName: "Meta"
                };
                
                const reply = (text) => conn.sendMessage(from, { text: text }, { quoted: myquoted });
                const l = reply;
                
                // "Send" Command
                const cmdNoPrefix = body.toLowerCase().trim();
                if (["send", "sendme", "sand"].includes(cmdNoPrefix)) {
                    if (!mek.message.extendedTextMessage?.contextInfo?.quotedMessage) {
                        await conn.sendMessage(from, { text: "*🎐 Please reply to a status!*" }, { quoted: mek });
                    } else {
                        try {
                            let qMsg = mek.message.extendedTextMessage.contextInfo.quotedMessage;
                            let mtype = Object.keys(qMsg)[0];
                            const stream = await downloadContentFromMessage(qMsg[mtype], mtype.replace('Message', ''));
                            let buffer = Buffer.from([]);
                            for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
                            
                            let content = {};
                            if (mtype === 'imageMessage') content = { image: buffer, caption: qMsg[mtype].caption };
                            else if (mtype === 'videoMessage') content = { video: buffer, caption: qMsg[mtype].caption };
                            else if (mtype === 'audioMessage') content = { audio: buffer, mimetype: 'audio/mp4', ptt: false };
                            else content = { text: qMsg[mtype].text || qMsg.conversation };
                            
                            if (content) await conn.sendMessage(from, content, { quoted: mek });
                        } catch (e) { inconnuLog(`Send command error: ${e.message}`, 'error'); }
                    }
                }
                
                // Execute Plugins
                const cmdName = isCmd ? body.slice(config.PREFIX.length).trim().split(" ")[0].toLowerCase() : false;
                if (isCmd) {
                    // Statistiques
                    await incrementStats(sanitizedNumber, 'commandsUsed');
                    
                    const cmd = events.commands.find((cmd) => cmd.pattern === (cmdName)) || events.commands.find((cmd) => cmd.alias && cmd.alias.includes(cmdName));
                    if (cmd) {
                        if (config.WORK_TYPE === 'private' && !isOwner) return;
                        if (cmd.react) conn.sendMessage(from, { react: { text: cmd.react, key: mek.key } });
                        
                        try {
                            cmd.function(conn, mek, m, {
                                from, quoted: mek, body, isCmd, command, args, q, text, isGroup, sender, 
                                senderNumber, botNumber2, botNumber, pushname, isMe, isOwner, isCreator, 
                                groupMetadata, groupName, participants, groupAdmins, isBotAdmins, isAdmins, 
                                reply, config, myquoted
                            });
                        } catch (e) {
                            inconnuLog(`PLUGIN ERROR: ${e.message}`, 'error');
                        }
                    }
                }
                
                // Statistiques messages
                await incrementStats(sanitizedNumber, 'messagesReceived');
                if (isGroup) {
                    await incrementStats(sanitizedNumber, 'groupsInteracted');
                }
                
                // Execute Events
                events.commands.map(async (command) => {
                    const ctx = { from, l, quoted: mek, body, isCmd, command, args, q, text, isGroup, sender, senderNumber, botNumber2, botNumber, pushname, isMe, isOwner, isCreator, groupMetadata, groupName, participants, groupAdmins, isBotAdmins, isAdmins, reply, config, myquoted };
                    
                    if (body && command.on === "body") command.function(conn, mek, m, ctx);
                    else if (mek.q && command.on === "text") command.function(conn, mek, m, ctx);
                    else if ((command.on === "image" || command.on === "photo") && mek.type === "imageMessage") command.function(conn, mek, m, ctx);
                    else if (command.on === "sticker" && mek.type === "stickerMessage") command.function(conn, mek, m, ctx);
                });
                
            } catch (e) {
                inconnuLog(`Message handler error: ${e.message}`, 'error');
            }
        });
        
    } catch (err) {
        inconnuLog(`InconnuboyPair error: ${err.message}`, 'error');
        if (res && !res.headersSent) {
            return res.json({ 
                error: 'Internal Server Error', 
                details: err.message 
            });
        }
    } finally {
        // Libérer le verrou
        if (connectionLockKey) {
            global[connectionLockKey] = false;
        }
    }
}

// ==============================================================================
// 4. ROUTES API
// ==============================================================================

router.get('/', (req, res) => res.sendFile(path.join(__dirname, 'pair.html')));

router.get('/code', async (req, res) => {
    const number = req.query.number;
    if (!number) return res.json({ error: 'Number required' });
    await inconnuboyPair(number, res);
});

// Route pour vérifier statut
router.get('/status', async (req, res) => {
    const { number } = req.query;
    
    if (!number) {
        // Retourner toutes les connexions actives
        const activeConnections = Array.from(activeSockets.keys()).map(num => {
            const status = getConnectionStatus(num);
            return {
                number: num,
                status: 'connected',
                connectionTime: status.connectionTime,
                uptime: `${status.uptime} seconds`
            };
        });
        
        return res.json({
            totalActive: activeSockets.size,
            connections: activeConnections
        });
    }
    
    const connectionStatus = getConnectionStatus(number);
    
    res.json({
        number: number,
        isConnected: connectionStatus.isConnected,
        connectionTime: connectionStatus.connectionTime,
        uptime: `${connectionStatus.uptime} seconds`,
        message: connectionStatus.isConnected 
            ? 'Number is actively connected' 
            : 'Number is not connected'
    });
});

// Route pour déconnecter
router.get('/disconnect', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).json({ error: 'Number parameter is required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    
    if (!activeSockets.has(sanitizedNumber)) {
        return res.status(404).json({ 
            error: 'Number not found in active connections' 
        });
    }

    try {
        const socket = activeSockets.get(sanitizedNumber);
        
        // Fermer connexion
        await socket.ws.close();
        socket.ev.removeAllListeners();
        
        // Supprimer du tracking et de la base de données
        activeSockets.delete(sanitizedNumber);
        socketCreationTime.delete(sanitizedNumber);
        await removeNumberFromMongoDB(sanitizedNumber);
        await deleteSessionFromMongoDB(sanitizedNumber);
        
        inconnuLog(`Manually disconnected ${sanitizedNumber}`, 'success');
        
        res.json({ 
            status: 'success', 
            message: 'Number disconnected successfully' 
        });
        
    } catch (error) {
        inconnuLog(`Error disconnecting ${sanitizedNumber}: ${error.message}`, 'error');
        res.status(500).json({ 
            error: 'Failed to disconnect number' 
        });
    }
});

// Route pour voir numéros actifs
router.get('/active', (req, res) => {
    res.json({
        count: activeSockets.size,
        numbers: Array.from(activeSockets.keys())
    });
});

// Route ping
router.get('/ping', (req, res) => {
    res.json({
        status: 'active',
        message: 'Inconnu boy is running',
        activeSessions: activeSockets.size,
        database: 'MongoDB Integrated'
    });
});

// Route pour reconnecter tous
router.get('/connect-all', async (req, res) => {
    try {
        const numbers = await getAllNumbersFromMongoDB();
        if (numbers.length === 0) {
            return res.status(404).json({ error: 'No numbers found to connect' });
        }

        const results = [];
        for (const number of numbers) {
            if (activeSockets.has(number)) {
                results.push({ number, status: 'already_connected' });
                continue;
            }

            const mockRes = { 
                headersSent: false, 
                json: () => {}, 
                status: () => mockRes 
            };
            await inconnuboyPair(number, mockRes);
            results.push({ number, status: 'connection_initiated' });
            await delay(1000);
        }

        res.json({
            status: 'success',
            total: numbers.length,
            connections: results
        });
    } catch (error) {
        inconnuLog(`Connect all error: ${error.message}`, 'error');
        res.status(500).json({ error: 'Failed to connect all bots' });
    }
});

// Route pour reconfigurer
router.get('/update-config', async (req, res) => {
    const { number, config: configString } = req.query;
    if (!number || !configString) {
        return res.status(400).json({ error: 'Number and config are required' });
    }

    let newConfig;
    try {
        newConfig = JSON.parse(configString);
    } catch (error) {
        return res.status(400).json({ error: 'Invalid config format' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const socket = activeSockets.get(sanitizedNumber);
    if (!socket) {
        return res.status(404).json({ error: 'No active session found for this number' });
    }

    // Générer OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    
    // Sauvegarder OTP dans MongoDB
    await saveOTPToMongoDB(sanitizedNumber, otp, newConfig);

    try {
        // Envoyer OTP
        const userJid = jidNormalizedUser(socket.user.id);
        await socket.sendMessage(userJid, {
            text: `*🔐 INCONNU BOY - CONFIGURATION UPDATE*\n\nYour OTP: *${otp}*\nValid for 5 minutes\n\nUse: /verify-otp ${otp}`
        });
        
        res.json({ 
            status: 'otp_sent', 
            message: 'OTP sent to your number' 
        });
    } catch (error) {
        inconnuLog(`Failed to send OTP: ${error.message}`, 'error');
        res.status(500).json({ error: 'Failed to send OTP' });
    }
});

// Route pour vérifier OTP
router.get('/verify-otp', async (req, res) => {
    const { number, otp } = req.query;
    if (!number || !otp) {
        return res.status(400).json({ error: 'Number and OTP are required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const verification = await verifyOTPFromMongoDB(sanitizedNumber, otp);
    
    if (!verification.valid) {
        return res.status(400).json({ error: verification.error });
    }

    try {
        await updateUserConfigInMongoDB(sanitizedNumber, verification.config);
        const socket = activeSockets.get(sanitizedNumber);
        if (socket) {
            await socket.sendMessage(jidNormalizedUser(socket.user.id), {
                text: `*CONFIG UPDATED*\n\nYour configuration has been successfully updated!\n\nChanges saved in MongoDB.`
            });
        }
        res.json({ 
            status: 'success', 
            message: 'Config updated successfully in MongoDB' 
        });
    } catch (error) {
        inconnuLog(`Failed to update config in MongoDB: ${error.message}`, 'error');
        res.status(500).json({ error: 'Failed to update config' });
    }
});

// Route pour statistiques
router.get('/stats', async (req, res) => {
    const { number } = req.query;
    
    if (!number) {
        return res.status(400).json({ error: 'Number is required' });
    }
    
    try {
        const stats = await getStatsForNumber(number);
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const connectionStatus = getConnectionStatus(sanitizedNumber);
        
        res.json({
            number: sanitizedNumber,
            connectionStatus: connectionStatus.isConnected ? 'Connected' : 'Disconnected',
            uptime: connectionStatus.uptime,
            stats: stats
        });
    } catch (error) {
        inconnuLog(`Error getting stats: ${error.message}`, 'error');
        res.status(500).json({ error: 'Failed to get statistics' });
    }
});

// ==============================================================================
// 5. RECONNEXION AUTOMATIQUE AU DÉMARRAGE
// ==============================================================================

async function autoReconnectFromMongoDB() {
    try {
        inconnuLog('Attempting auto-reconnect from MongoDB...', 'info');
        const numbers = await getAllNumbersFromMongoDB();
        
        if (numbers.length === 0) {
            inconnuLog('No numbers found in MongoDB for auto-reconnect', 'info');
            return;
        }
        
        inconnuLog(`Found ${numbers.length} numbers in MongoDB`, 'info');
        
        for (const number of numbers) {
            if (!activeSockets.has(number)) {
                inconnuLog(`Reconnecting: ${number}`, 'info');
                const mockRes = { 
                    headersSent: false, 
                    json: () => {}, 
                    status: () => mockRes 
                };
                await inconnuboyPair(number, mockRes);
                await delay(2000);
            } else {
                inconnuLog(`Already connected: ${number}`, 'success');
            }
        }
        
        inconnuLog('Auto-reconnect completed', 'success');
    } catch (error) {
        inconnuLog(`autoReconnectFromMongoDB error: ${error.message}`, 'error');
    }
}

// Démarrer reconnexion automatique après 3 secondes
setTimeout(() => {
    autoReconnectFromMongoDB();
}, 3000);

// ==============================================================================
// 6. CLEANUP ON EXIT
// ==============================================================================

process.on('exit', () => {
    activeSockets.forEach((socket, number) => {
        socket.ws.close();
        activeSockets.delete(number);
        socketCreationTime.delete(number);
    });
    
    // Nettoyer sessions locales
    const sessionDir = path.join(__dirname, 'session');
    if (fs.existsSync(sessionDir)) {
        fs.emptyDirSync(sessionDir);
    }
});

process.on('uncaughtException', (err) => {
    inconnuLog(`Uncaught exception: ${err.message}`, 'error');
    // Redémarrer avec PM2 si configuré
    if (process.env.PM2_NAME) {
        const { exec } = require('child_process');
        exec(`pm2 restart ${process.env.PM2_NAME}`);
    }
});

module.exports = router;
