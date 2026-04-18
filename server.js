require('dotenv').config();
const express = require('express');
const session = require('express-session');
const path = require('path');
const fs = require('fs');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const {
  Client, GatewayIntentBits, Partials, PermissionsBitField,
  EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle,
  SlashCommandBuilder, REST, Routes, ChannelType
} = require('discord.js');

const app = express();
const PORT = process.env.PORT || 3000;
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildModeration,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel, Partials.Message]
});
const upload = multer({ dest: path.join(__dirname, 'uploads'), limits: { fileSize: 8 * 1024 * 1024 } });

const DATA = path.join(__dirname, 'data');
const SETTINGS = path.join(DATA, 'settings.json');
const USERS = path.join(DATA, 'panel_users.json');
const LOGS = path.join(DATA, 'activity_logs.json');
const PANEL_PERMS = path.join(DATA, 'panel_perms.json');
const SUBSCRIBERS = path.join(DATA, 'subscribers.json');
const LOG_CHANNELS = path.join(DATA, 'log_channels.json');

if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });
function ensure(file, value){ if (!fs.existsSync(file)) fs.writeFileSync(file, JSON.stringify(value, null, 2), 'utf8'); }
ensure(LOG_CHANNELS, { guildId: '', categoryId: '', channels: {} });

const read = (f) => JSON.parse(fs.readFileSync(f, 'utf8'));
const write = (f, d) => fs.writeFileSync(f, JSON.stringify(d, null, 2), 'utf8');
const clean = (v='') => String(v).trim();

const LOG_BLUEPRINT = {
  category: 'WM STORE LOGS',
  channels: {
    login: 'login-logs',
    announcement: 'announcement-logs',
    subscription: 'subscription-logs',
    permissions: 'permissions-logs',
    member: 'member-logs',
    message: 'message-logs',
    channel: 'channel-logs',
    role: 'role-logs',
    voice: 'voice-logs',
    moderation: 'moderation-logs',
    server: 'server-logs',
    system: 'system-logs',
    error: 'error-logs'
  }
};

function resolveLogBucket(type=''){
  if (type.includes('login') || type.includes('logout')) return 'login';
  if (type.includes('announcement')) return 'announcement';
  if (type.includes('subscriber') || type.includes('subscription')) return 'subscription';
  if (type.includes('perm')) return 'permissions';
  if (type.includes('member_')) return 'member';
  if (type.includes('message_')) return 'message';
  if (type.includes('channel_')) return 'channel';
  if (type.includes('role_')) return 'role';
  if (type.includes('voice_')) return 'voice';
  if (type.includes('ban_') || type.includes('mod_') || type.includes('timeout_')) return 'moderation';
  if (type.includes('server_')) return 'server';
  if (type.includes('error')) return 'error';
  return 'system';
}

async function ensureLogInfrastructure(){
  const logGuildId = clean(process.env.LOG_GUILD_ID);
  if (!logGuildId) return null;

  let guild;
  try {
    guild = await client.guilds.fetch(logGuildId);
    await guild.channels.fetch();
  } catch {
    console.error('LOG_GUILD_ID invalid or bot not inside logs guild.');
    return null;
  }

  let state = read(LOG_CHANNELS);
  let category = state.categoryId ? guild.channels.cache.get(state.categoryId) : null;
  if (!category) {
    category = guild.channels.cache.find(c => c.type === ChannelType.GuildCategory && c.name === LOG_BLUEPRINT.category) || null;
  }
  if (!category) {
    category = await guild.channels.create({ name: LOG_BLUEPRINT.category, type: ChannelType.GuildCategory });
  }

  const out = { guildId: guild.id, categoryId: category.id, channels: {} };

  for (const [key, chName] of Object.entries(LOG_BLUEPRINT.channels)) {
    let ch = guild.channels.cache.find(c => c.parentId === category.id && c.type === ChannelType.GuildText && c.name === chName) || null;
    if (!ch) {
      ch = await guild.channels.create({ name: chName, type: ChannelType.GuildText, parent: category.id });
    }
    out.channels[key] = ch.id;
  }

  write(LOG_CHANNELS, out);
  return out;
}

async function sendLogToDiscord(logData){
  const state = read(LOG_CHANNELS);
  if (!state.guildId || !state.channels) return;
  const bucket = resolveLogBucket(logData.type || '');
  const channelId = state.channels[bucket] || state.channels.system;
  if (!channelId) return;
  try {
    const channel = await client.channels.fetch(channelId);
    if (!channel) return;
    const embed = new EmbedBuilder()
      .setColor('#7c3aed')
      .setTitle(`WM STORE LOG | ${bucket.toUpperCase()}`)
      .addFields(
        { name: 'Type', value: String(logData.type || 'unknown').slice(0,1024), inline: true },
        { name: 'User', value: String(logData.actorEmail || logData.actorTag || '-').slice(0,1024), inline: true },
        { name: 'Time', value: new Date(logData.time).toLocaleString('de-DE'), inline: false },
        { name: 'Details', value: String(logData.message || logData.plainTextPreview || '-').slice(0,1024), inline: false }
      )
      .setTimestamp(new Date(logData.time));
    await channel.send({ embeds: [embed] });
  } catch (err) {
    console.error('Discord log send failed:', err.message);
  }
}

function addLog(entry){
  const logs = read(LOGS);
  const logData = { id:String(Date.now()), time:new Date().toISOString(), ...entry };
  logs.unshift(logData);
  write(LOGS, logs.slice(0,2000));
  sendLogToDiscord(logData).catch(() => {});
}

function me(req){ return read(USERS).find(u => u.id === req.session.userId) || null; }
function auth(req,res,next){ if(!req.session.userId) return res.redirect('/login'); next(); }
function ownerOrAdmin(req,res,next){ const u=me(req); if(!u || !['owner','admin'].includes(u.role)) return res.status(403).send('Forbidden'); next(); }
function owner(req,res,next){ const u=me(req); if(!u || u.role!=='owner') return res.status(403).send('Forbidden'); next(); }
function logoPath(){ return fs.existsSync(path.join(__dirname, 'public', 'logo.png')) ? '/static/logo.png' : '/static/logo.svg'; }

async function developerProfile(){
  const s = read(SETTINGS);
  const fallback = { displayName:s.fallbackDeveloperName, username:s.fallbackDeveloperUsername, avatar:s.fallbackDeveloperAvatar };
  const developerId = clean(process.env.DEVELOPER_DISCORD_ID);
  if(!developerId) return fallback;
  try {
    const user = await client.users.fetch(developerId, { force:true });
    return {
      displayName: user.globalName || user.displayName || user.username || fallback.displayName,
      username: user.username || fallback.username,
      avatar: user.displayAvatarURL({ extension:'png', size:512 }) || fallback.avatar
    };
  } catch { return fallback; }
}

app.locals.logoPath = logoPath;
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use('/static', express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
app.use(express.urlencoded({ extended:true }));
app.use(express.json());
app.use(helmet({ contentSecurityPolicy:false }));
app.use(session({
  secret: process.env.SESSION_SECRET || 'WMSTORE_SECRET_2026',
  resave:false, saveUninitialized:false,
  cookie:{ httpOnly:true, sameSite:'lax', secure:false, maxAge:1000*60*60*12 }
}));
app.use(rateLimit({ windowMs:15*60*1000, max:350 }));

async function guildChoices(){ return client.guilds.cache.map(g => ({ id:g.id, name:g.name, members:g.memberCount || 0 })).sort((a,b)=>a.name.localeCompare(b.name)); }
const guildDataCache = new Map();
async function guildData(gid){
  const now = Date.now(); const cached = guildDataCache.get(gid);
  if(cached && now - cached.time < 30000) return cached.data;
  const guild = await client.guilds.fetch(gid); await guild.channels.fetch();
  const roles = guild.roles.cache.filter(r=>r.name!=='@everyone').sort((a,b)=>b.position-a.position).map(r=>({id:r.id,name:r.name,count:r.members.size}));
  const channels = guild.channels.cache.filter(c=>c.type===0).map(c=>({id:c.id,name:c.name})).sort((a,b)=>a.name.localeCompare(b.name));
  const data = { guild, roles, channels }; guildDataCache.set(gid, { time:now, data }); return data;
}
async function canUserAnnounceInGuild(req, guildId){
  const currentUser = me(req); if(!currentUser) return false; if(currentUser.role==='owner') return true;
  const perms = read(PANEL_PERMS); const requiredRoleId = perms[guildId];
  if(!requiredRoleId || !currentUser.discordUserId) return false;
  try {
    const guild = await client.guilds.fetch(guildId);
    const members = await guild.members.fetch();
    const member = members.get(currentUser.discordUserId);
    if(!member) return false;
    return member.roles.cache.has(requiredRoleId);
  } catch { return false; }
}
function buildMentionText(type, roleId){ if(type==='everyone') return '@everyone'; if(type==='here') return '@here'; if(type==='role' && roleId) return `<@&${roleId}>`; return ''; }
function buildAnnouncementPayload(body){
  const mentionText = buildMentionText(clean(body.mentionType), clean(body.mentionRoleId));
  const imageUrl = clean(body.imageUrl);
  if(body.useEmbed === 'on'){
    const embed = new EmbedBuilder().setColor(clean(body.color || '#7c3aed')).setDescription(clean(body.description) || ' ').setTimestamp();
    if(clean(body.title)) embed.setTitle(clean(body.title));
    if(clean(body.footer)) embed.setFooter({ text: clean(body.footer) });
    if(imageUrl) embed.setImage(imageUrl);
    const payload = { embeds:[embed] };
    if(mentionText || clean(body.plainText)) payload.content = [mentionText, clean(body.plainText)].filter(Boolean).join('\\n');
    return payload;
  }
  return { content: [mentionText, clean(body.plainText), clean(body.description)].filter(Boolean).join('\\n') || ' ' };
}

const sendQueueState = { isRunning:false, isPaused:false, processed:0, success:0, failed:0, total:0, errors:[], startedAt:null, finishedAt:null, mode:null };
function sleep(ms){ return new Promise(resolve => setTimeout(resolve, ms)); }
async function sendWithRetry(user, payload, maxRetries=2){
  let attempt = 0;
  while(attempt <= maxRetries){
    try { await user.send(payload); return { ok:true }; }
    catch(err){
      const msg = String(err?.message || err); const code = err?.code || '';
      if(code===50007 || msg.includes('Cannot send messages to this user')) return { ok:false, permanent:true, error:msg };
      attempt++; if(attempt>maxRetries) return { ok:false, permanent:false, error:msg };
      await sleep(2500*attempt);
    }
  }
  return { ok:false, permanent:false, error:'Unknown error' };
}
async function sendProfessionalQueue(targets, payload, options={}){
  const baseDelay = Number(options.baseDelay || 2200), batchSize = Number(options.batchSize || 5), batchPause = Number(options.batchPause || 9000), maxRetries = Number(options.maxRetries || 2);
  sendQueueState.isRunning = true; sendQueueState.isPaused = false; sendQueueState.processed = 0; sendQueueState.success = 0; sendQueueState.failed = 0; sendQueueState.total = targets.length; sendQueueState.errors = []; sendQueueState.startedAt = new Date().toISOString(); sendQueueState.finishedAt = null;
  for(let i=0;i<targets.length;i++){
    if(!sendQueueState.isRunning) break;
    while(sendQueueState.isPaused) await sleep(1000);
    const user = targets[i]; const result = await sendWithRetry(user, payload, maxRetries);
    sendQueueState.processed++;
    if(result.ok) sendQueueState.success++;
    else { sendQueueState.failed++; sendQueueState.errors.push({ userId:user.id, tag:user.tag || user.username || user.id, error:result.error }); addLog({ type:'error_dm_send', actorEmail:'bot', message:`${user.id} -> ${result.error}` }); }
    await sleep(baseDelay);
    if((i+1)%batchSize===0 && i+1<targets.length) await sleep(batchPause);
  }
  sendQueueState.isRunning = false; sendQueueState.finishedAt = new Date().toISOString();
  return { total:sendQueueState.total, success:sendQueueState.success, failed:sendQueueState.failed, errors:sendQueueState.errors.slice(0,50) };
}
async function sendToOptInSubscribers(payload){
  const subscribers = read(SUBSCRIBERS).filter(s => s.active && s.discordUserId);
  const targets = [];
  for(const s of subscribers){
    try{ const user = await client.users.fetch(s.discordUserId, { force:false }); if(user && !user.bot) targets.push(user); } catch{}
  }
  if(!targets.length) throw new Error('لا يوجد مشتركون مفعلون حالياً.');
  sendQueueState.mode = 'opt_in_subscribers';
  return sendProfessionalQueue(targets, payload, { baseDelay:2200, batchSize:5, batchPause:9000, maxRetries:2 });
}
function upsertSubscriber(user){
  const subs = read(SUBSCRIBERS); const existing = subs.find(s => s.discordUserId === user.id);
  if(existing){
    existing.active = true; existing.username = user.username; existing.displayName = user.globalName || user.username; existing.updatedAt = new Date().toISOString();
  } else {
    subs.unshift({ id:String(Date.now()), discordUserId:user.id, username:user.username, displayName:user.globalName || user.username, active:true, source:'discord_opt_in_panel', optedAt:new Date().toISOString(), updatedAt:new Date().toISOString() });
  }
  write(SUBSCRIBERS, subs);
}
function disableSubscriber(user){
  const subs = read(SUBSCRIBERS); const existing = subs.find(s => s.discordUserId === user.id);
  if(existing){ existing.active = false; existing.updatedAt = new Date().toISOString(); write(SUBSCRIBERS, subs); }
}
function subscriptionPanelEmbed(){
  const s = read(SETTINGS);
  return new EmbedBuilder().setColor(s.brandPrimary || '#7c3aed').setTitle(s.subscriptionPanelTitle).setDescription(s.subscriptionPanelDescription).setFooter({ text:s.subscriptionPanelFooter }).setTimestamp();
}
function subscriptionPanelRows(){
  return [new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('wm_subscribe_announcements').setLabel('اشتراك بالإعلانات').setEmoji('✅').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('wm_unsubscribe_announcements').setLabel('إلغاء الاشتراك').setEmoji('🔕').setStyle(ButtonStyle.Secondary)
  )];
}
async function registerCommands(){
  if(!process.env.CLIENT_ID) return;
  const commands = [new SlashCommandBuilder().setName('setup-subscription-panel').setDescription('إرسال بانل الاشتراك في الإعلانات داخل هذه القناة').setDefaultMemberPermissions(PermissionsBitField.Flags.Administrator)].map(c => c.toJSON());
  const rest = new REST({ version:'10' }).setToken(process.env.BOT_TOKEN);
  if(process.env.SUBSCRIPTION_GUILD_ID){
    await rest.put(Routes.applicationGuildCommands(process.env.CLIENT_ID, process.env.SUBSCRIPTION_GUILD_ID), { body:commands });
  } else {
    await rest.put(Routes.applicationCommands(process.env.CLIENT_ID), { body:commands });
  }
}

app.get('/login', async (req,res) => res.render('login', { error:null, settings:read(SETTINGS), developer:await developerProfile() }));
app.post('/login', rateLimit({ windowMs:15*60*1000, max:20 }), async (req,res) => {
  const email = clean(req.body.email).toLowerCase(); const password = clean(req.body.password);
  const user = read(USERS).find(u => clean(u.email).toLowerCase()===email && clean(u.password)===password && u.isActive);
  if(!user) return res.render('login', { error:'بيانات الدخول غير صحيحة.', settings:read(SETTINGS), developer:await developerProfile() });
  req.session.userId = user.id; addLog({ type:'login_success', actorEmail:user.email, message:'Panel login success' }); res.redirect('/');
});
app.get('/logout', auth, (req,res) => { const user = me(req); if(user) addLog({ type:'logout_success', actorEmail:user.email, message:'Panel logout' }); req.session.destroy(() => res.redirect('/login')); });

app.get('/', auth, ownerOrAdmin, async (req,res) => {
  res.render('dashboard', { currentUser:me(req), settings:read(SETTINGS), guilds:await guildChoices(), logs:read(LOGS).slice(0,40), users:read(USERS), subscribers:read(SUBSCRIBERS).slice(0,100), subscribersCount:read(SUBSCRIBERS).filter(s => s.active).length, developer:await developerProfile(), panelPerms:read(PANEL_PERMS), result:null });
});
app.get('/guild-data/:gid', auth, ownerOrAdmin, async (req,res) => {
  try{ const d = await guildData(req.params.gid); res.json({ ok:true, roles:d.roles, channels:d.channels, guildName:d.guild.name }); }
  catch(err){ addLog({ type:'error_guild_data', actorEmail: me(req)?.email || '-', message: err.message }); res.status(500).json({ ok:false, error:err.message }); }
});
app.post('/announce', auth, ownerOrAdmin, upload.single('image'), async (req,res) => {
  const currentUser = me(req); const guildId = clean(req.body.guildId); const mode = clean(req.body.sendMode || 'channel');
  if(!(await canUserAnnounceInGuild(req, guildId))){
    return res.render('dashboard', { currentUser, settings:read(SETTINGS), guilds:await guildChoices(), logs:read(LOGS).slice(0,40), users:read(USERS), subscribers:read(SUBSCRIBERS).slice(0,100), subscribersCount:read(SUBSCRIBERS).filter(s => s.active).length, developer:await developerProfile(), panelPerms:read(PANEL_PERMS), result:{ error:'ليس لديك صلاحية الإعلانات في هذا السيرفر.' } });
  }
  try{
    const guild = await client.guilds.fetch(guildId); await guild.channels.fetch();
    const imageUrl = req.file ? `${req.protocol}://${req.get('host')}/uploads/${req.file.filename}` : clean(req.body.imageUrl);
    const payload = buildAnnouncementPayload({ ...req.body, imageUrl });
    if(mode === 'channel'){
      const channelId = clean(req.body.channelId); const channel = guild.channels.cache.get(channelId);
      if(!channel) throw new Error('قناة الإعلانات غير موجودة.');
      await channel.send(payload);
      addLog({ type:'announcement_channel_send', actorEmail:currentUser.email, guildId, channelId, message:`Announcement sent to channel ${channelId}` });
    } else if(mode === 'subscribers'){
      const result = await sendToOptInSubscribers(payload);
      addLog({ type:'announcement_optin_dm_send', actorEmail:currentUser.email, guildId, message:`DM sent to opt-in subscribers: ${result.success} success / ${result.failed} failed` });
    } else throw new Error('نوع الإرسال غير صحيح.');
    res.render('dashboard', { currentUser, settings:read(SETTINGS), guilds:await guildChoices(), logs:read(LOGS).slice(0,40), users:read(USERS), subscribers:read(SUBSCRIBERS).slice(0,100), subscribersCount:read(SUBSCRIBERS).filter(s => s.active).length, developer:await developerProfile(), panelPerms:read(PANEL_PERMS), result:{ success:true, message:'تم تنفيذ الإرسال بنجاح.' } });
  } catch(err){
    addLog({ type:'error_announcement', actorEmail: currentUser?.email || '-', message: err.message });
    res.render('dashboard', { currentUser, settings:read(SETTINGS), guilds:await guildChoices(), logs:read(LOGS).slice(0,40), users:read(USERS), subscribers:read(SUBSCRIBERS).slice(0,100), subscribersCount:read(SUBSCRIBERS).filter(s => s.active).length, developer:await developerProfile(), panelPerms:read(PANEL_PERMS), result:{ error:err.message } });
  }
});
app.get('/send-status', auth, ownerOrAdmin, (req,res) => res.json(sendQueueState));
app.post('/send-pause', auth, ownerOrAdmin, (req,res) => { sendQueueState.isPaused = true; res.json({ ok:true }); });
app.post('/send-resume', auth, ownerOrAdmin, (req,res) => { sendQueueState.isPaused = false; res.json({ ok:true }); });
app.post('/send-stop', auth, ownerOrAdmin, (req,res) => { sendQueueState.isRunning = false; sendQueueState.isPaused = false; sendQueueState.finishedAt = new Date().toISOString(); res.json({ ok:true }); });
app.post('/panel-perms/save', auth, owner, (req,res) => {
  const perms = read(PANEL_PERMS); const guildId = clean(req.body.guildId); const roleId = clean(req.body.roleId);
  if(guildId && roleId){ perms[guildId] = roleId; write(PANEL_PERMS, perms); addLog({ type:'permissions_update', actorEmail:me(req).email, message:`Set announcer role for guild ${guildId}` }); }
  res.redirect('/');
});
app.post('/users/add', auth, owner, (req,res) => {
  const u = read(USERS); u.push({ id:String(Date.now()), email:clean(req.body.email).toLowerCase(), password:clean(req.body.password), role:clean(req.body.role) || 'admin', discordUserId:clean(req.body.discordUserId), isActive:true });
  write(USERS, u); addLog({ type:'system_user_add', actorEmail: me(req).email, message:`Added panel user ${clean(req.body.email)}` }); res.redirect('/');
});
app.post('/users/toggle/:id', auth, owner, (req,res) => {
  const u = read(USERS); const t = u.find(x => x.id === req.params.id); if(t) t.isActive = !t.isActive; write(USERS, u); addLog({ type:'system_user_toggle', actorEmail: me(req).email, message:`Toggled user ${req.params.id}` }); res.redirect('/');
});
app.post('/settings', auth, owner, (req,res) => {
  const s = read(SETTINGS); s.appName='WM STORE'; s.siteDescription=clean(req.body.siteDescription)||s.siteDescription; s.backgroundImageUrl=clean(req.body.backgroundImageUrl); s.blurStrength=Number(req.body.blurStrength||s.blurStrength||14); s.defaultMentionType=clean(req.body.defaultMentionType||s.defaultMentionType||'none'); s.defaultAnnouncementChannelId=clean(req.body.defaultAnnouncementChannelId||s.defaultAnnouncementChannelId||''); s.subscriptionPanelTitle=clean(req.body.subscriptionPanelTitle||s.subscriptionPanelTitle); s.subscriptionPanelDescription=clean(req.body.subscriptionPanelDescription||s.subscriptionPanelDescription); s.subscriptionPanelFooter=clean(req.body.subscriptionPanelFooter||s.subscriptionPanelFooter); write(SETTINGS, s); addLog({ type:'system_settings_update', actorEmail: me(req).email, message:'Updated panel settings' }); res.redirect('/');
});

client.on('interactionCreate', async interaction => {
  try{
    if(interaction.isChatInputCommand() && interaction.commandName === 'setup-subscription-panel'){
      await interaction.channel.send({ embeds:[subscriptionPanelEmbed()], components:subscriptionPanelRows() });
      addLog({ type:'subscription_panel_posted', actorEmail:'bot', message:`Subscription panel posted in channel ${interaction.channelId}` });
      return interaction.reply({ content:'تم إرسال بانل الاشتراك بنجاح.', ephemeral:true });
    }
    if(interaction.isButton()){
      if(interaction.customId === 'wm_subscribe_announcements'){
        upsertSubscriber(interaction.user);
        addLog({ type:'subscriber_opt_in', actorEmail:'bot', message:`${interaction.user.username} opted in to announcements` });
        return interaction.reply({ content:'تم اشتراكك في إعلانات WM STORE. من الآن البوت يرسل لك الإعلانات بالخاص لأنك وافقت على ذلك.', ephemeral:true });
      }
      if(interaction.customId === 'wm_unsubscribe_announcements'){
        disableSubscriber(interaction.user);
        addLog({ type:'subscriber_opt_out', actorEmail:'bot', message:`${interaction.user.username} opted out from announcements` });
        return interaction.reply({ content:'تم إلغاء اشتراكك من إعلانات WM STORE.', ephemeral:true });
      }
    }
  } catch(err){
    addLog({ type:'error_interaction', actorEmail:'bot', message: err.message });
    if(!interaction.deferred && !interaction.replied){ try{ await interaction.reply({ content:'صار خطأ أثناء تنفيذ العملية.', ephemeral:true }); } catch{} }
  }
});

client.on('guildMemberAdd', member => addLog({ type:'member_join', actorTag: member.user.tag, message:`${member.user.tag} joined ${member.guild.name}` }));
client.on('guildMemberRemove', member => addLog({ type:'member_leave', actorTag: member.user?.tag || member.id, message:`${member.user?.tag || member.id} left ${member.guild.name}` }));
client.on('guildMemberUpdate', (oldMember, newMember) => {
  if (oldMember.nickname !== newMember.nickname) addLog({ type:'member_nickname_update', actorTag:newMember.user.tag, message:`Nickname changed: ${oldMember.nickname || '-'} -> ${newMember.nickname || '-'}` });
  const oldRoles = [...oldMember.roles.cache.keys()].filter(id => id !== oldMember.guild.id);
  const newRoles = [...newMember.roles.cache.keys()].filter(id => id !== newMember.guild.id);
  const added = newRoles.filter(id => !oldRoles.includes(id));
  const removed = oldRoles.filter(id => !newRoles.includes(id));
  if (added.length) addLog({ type:'member_role_add', actorTag:newMember.user.tag, message:`Added roles to ${newMember.user.tag}: ${added.join(', ')}` });
  if (removed.length) addLog({ type:'member_role_remove', actorTag:newMember.user.tag, message:`Removed roles from ${newMember.user.tag}: ${removed.join(', ')}` });
});
client.on('messageDelete', message => { if (message.author?.bot) return; addLog({ type:'message_delete', actorTag: message.author?.tag || '-', message:`Deleted in #${message.channel?.name || 'unknown'}: ${(message.content || '[embed/attachment]').slice(0, 500)}` }); });
client.on('messageUpdate', (oldMsg, newMsg) => { if (oldMsg.author?.bot || newMsg.author?.bot) return; if ((oldMsg.content || '') === (newMsg.content || '')) return; addLog({ type:'message_edit', actorTag:newMsg.author?.tag || '-', message:`Edited in #${newMsg.channel?.name || 'unknown'}: ${(oldMsg.content || '-').slice(0,200)} -> ${(newMsg.content || '-').slice(0,200)}` }); });
client.on('channelCreate', channel => addLog({ type:'channel_create', actorTag:'system', message:`Channel created: ${channel.name}` }));
client.on('channelDelete', channel => addLog({ type:'channel_delete', actorTag:'system', message:`Channel deleted: ${channel.name}` }));
client.on('channelUpdate', (oldChannel, newChannel) => { if (oldChannel.name !== newChannel.name) addLog({ type:'channel_rename', actorTag:'system', message:`Channel renamed: ${oldChannel.name} -> ${newChannel.name}` }); });
client.on('roleCreate', role => addLog({ type:'role_create', actorTag:'system', message:`Role created: ${role.name}` }));
client.on('roleDelete', role => addLog({ type:'role_delete', actorTag:'system', message:`Role deleted: ${role.name}` }));
client.on('roleUpdate', (oldRole, newRole) => { if (oldRole.name !== newRole.name) addLog({ type:'role_rename', actorTag:'system', message:`Role renamed: ${oldRole.name} -> ${newRole.name}` }); });
client.on('voiceStateUpdate', (oldState, newState) => {
  const userTag = newState.member?.user?.tag || oldState.member?.user?.tag || 'unknown';
  if (oldState.channelId !== newState.channelId) addLog({ type:'voice_move', actorTag:userTag, message:`Voice move: ${oldState.channel?.name || 'none'} -> ${newState.channel?.name || 'none'}` });
  else if (oldState.serverMute !== newState.serverMute) addLog({ type:'voice_server_mute', actorTag:userTag, message:`Server mute changed for ${userTag}: ${newState.serverMute}` });
  else if (oldState.serverDeaf !== newState.serverDeaf) addLog({ type:'voice_server_deaf', actorTag:userTag, message:`Server deaf changed for ${userTag}: ${newState.serverDeaf}` });
});
client.on('guildBanAdd', ban => addLog({ type:'ban_add', actorTag:ban.user?.tag || '-', message:`User banned: ${ban.user?.tag || ban.user?.id}` }));
client.on('guildBanRemove', ban => addLog({ type:'ban_remove', actorTag:ban.user?.tag || '-', message:`User unbanned: ${ban.user?.tag || ban.user?.id}` }));
client.on('guildUpdate', (oldGuild, newGuild) => { if (oldGuild.name !== newGuild.name) addLog({ type:'server_rename', actorTag:'system', message:`Server renamed: ${oldGuild.name} -> ${newGuild.name}` }); });

client.once('ready', async () => {
  console.log(`WM STORE bot logged in as ${client.user.tag}`);
  try { await ensureLogInfrastructure(); console.log('Log infrastructure ready.'); } catch (err) { console.error('Log infrastructure failed:', err.message); }
  try { await registerCommands(); console.log('Slash commands registered.'); } catch(err){ console.error('Register commands failed:', err.message); }
  addLog({ type:'system_startup', actorTag:'bot', message:'WM STORE bot started successfully' });
});

client.login(process.env.BOT_TOKEN);
app.listen(PORT, () => console.log(`WM STORE Panel: http://localhost:${PORT}`));
