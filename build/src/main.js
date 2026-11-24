"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const utils = __importStar(require("@iobroker/adapter-core"));
const mqttClient_1 = require("../lib/mqttClient");
const deviceDiscovery_1 = require("../lib/deviceDiscovery");
const stateManager_1 = require("../lib/stateManager");
const topicBuilder_1 = require("../lib/topicBuilder");
const fieldDefinitions_1 = require("../lib/fieldDefinitions");
class FaikoutAdapter extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'faikout-ac' });
        this.mqttClient = null;
        this.stateManagers = new Map();
        this.topicToFolder = new Map();
        this.folderToTopics = new Map();
        this.demandCtxMap = new Map();
        this.outdoorIdToFolders = new Map();
        this.roomIdToFolders = new Map();
        this.scheduleFired = new Map();
        this.humidityStates = new Map();
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }
    async onReady() {
        await this.setStateAsync('info.connection', { val: false, ack: true });
        const devices = this._sanitizeDeviceNames(this.config.devices ?? []);
        const enabledDevices = devices.filter((d) => d.enabled !== false);
        if (enabledDevices.length === 0) {
            this.log.warn('No devices configured. Please add at least one device in the adapter settings.');
        }
        for (const device of enabledDevices) {
            const folder = device.name.replace(/[^a-zA-Z0-9_-]/g, '_');
            const sm = new stateManager_1.StateManager(this, folder);
            this.stateManagers.set(folder, sm);
            await this._restoreTopicFromStates(folder);
        }
        const mqttHost = this.config.mqttHost ?? 'localhost';
        const mqttPort = this.config.mqttPort ?? 1883;
        const mqttUser = this.config.mqttUser;
        const mqttPassword = this.config.mqttPassword;
        const instanceId = this.instance ?? 0;
        this.mqttClient = new mqttClient_1.FaikoutMqttClient({
            host: mqttHost,
            port: mqttPort,
            username: mqttUser || undefined,
            password: mqttPassword || undefined,
            clientId: `iobroker-faikout-${instanceId}`,
            setTimeoutFn: (cb, ms) => this.setTimeout(cb, ms),
            clearTimeoutFn: (id) => this.clearTimeout(id),
        }, (topic, payload) => {
            void this._handleMqttMessage(topic, payload).catch((e) => this.log.error(`MQTT message error: ${e}`));
        }, (connected) => {
            void this._handleConnectionChange(connected).catch((e) => this.log.error(`Connection change error: ${e}`));
        });
        this.mqttClient.connect();
        for (const topic of this.topicToFolder.keys()) {
            this.mqttClient.subscribe(topic);
            this.log.info(`Subscribed to: ${topic}`);
        }
        await this._setupDemandControl();
        this._setupEnvSensors();
        this._setupSchedule();
        await this._setupHumidityControls();
        await this.subscribeStatesAsync('*');
        for (const folder of this.demandCtxMap.keys()) {
            void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
        }
    }
    _sanitizeDeviceNames(devices) {
        const usedNames = new Set();
        return devices.map((d) => {
            const base = (d.name ?? '').trim() || 'MyDaikin';
            let name = base;
            let counter = 2;
            while (usedNames.has(name.toLowerCase())) {
                name = `${base}${counter++}`;
            }
            if (name !== d.name) {
                this.log.warn(`Device name "${d.name}" was empty or duplicate – using "${name}" instead`);
            }
            usedNames.add(name.toLowerCase());
            return { ...d, name };
        });
    }
    async _restoreTopicFromStates(folder) {
        try {
            const deviceIdState = await this.getStateAsync(`${folder}.info.deviceId`);
            const topicStateState = await this.getStateAsync(`${folder}.topics.state`);
            const topicCommandState = await this.getStateAsync(`${folder}.topics.command`);
            const topicSettingState = await this.getStateAsync(`${folder}.topics.setting`);
            const prefixAppState = await this.getStateAsync(`${folder}.topics.prefixApp`);
            const prefixHostState = await this.getStateAsync(`${folder}.topics.prefixHost`);
            const appNameState = await this.getStateAsync(`${folder}.info.appName`);
            if (!deviceIdState?.val) {
                this.log.info(`No stored discovery data for "${folder}" – use the Discover button in Admin UI`);
                return;
            }
            const deviceId = String(deviceIdState.val);
            const appName = appNameState?.val ? String(appNameState.val) : '';
            const topicConfig = {
                topicstate: topicStateState?.val ? String(topicStateState.val) : topicBuilder_1.DEFAULT_TOPIC_CONFIG.topicstate,
                topiccommand: topicCommandState?.val
                    ? String(topicCommandState.val)
                    : topicBuilder_1.DEFAULT_TOPIC_CONFIG.topiccommand,
                topicsetting: topicSettingState?.val
                    ? String(topicSettingState.val)
                    : topicBuilder_1.DEFAULT_TOPIC_CONFIG.topicsetting,
                prefixapp: prefixAppState?.val === true,
                prefixhost: prefixHostState?.val === true,
            };
            const topics = (0, topicBuilder_1.buildTopicPaths)(deviceId, appName, topicConfig);
            this.topicToFolder.set(topics.statusTopic, folder);
            this.folderToTopics.set(folder, topics);
            this.log.info(`Restored topic for "${folder}": ${topics.statusTopic}`);
        }
        catch (err) {
            this.log.warn(`Could not restore topic for "${folder}": ${String(err)}`);
        }
    }
    async _handleMqttMessage(topic, payload) {
        const folder = this.topicToFolder.get(topic);
        if (!folder) {
            this.log.debug(`Received message on unknown topic: ${topic}`);
            return;
        }
        const sm = this.stateManagers.get(folder);
        if (!sm) {
            return;
        }
        const deviceObj = await this.getObjectAsync(folder);
        if (!deviceObj) {
            this.log.debug(`Skipping MQTT message for "${folder}" – run Discovery first`);
            return;
        }
        try {
            const json = JSON.parse(payload);
            if (json.up === false) {
                await this.setStateAsync(`${folder}.info.connection`, { val: false, ack: true });
                this.log.info(`Device "${folder}" disconnected (LWT)`);
                return;
            }
        }
        catch {
        }
        await sm.updateFromMqttPayload(payload);
        await this.setStateAsync(`${folder}.info.connection`, { val: true, ack: true });
    }
    async _handleConnectionChange(connected) {
        await this.setStateAsync('info.connection', { val: connected, ack: true });
        if (connected) {
            this.log.info('MQTT connected');
            for (const ctx of this.demandCtxMap.values()) {
                this._publishDemandConfigParam(ctx.folder, 'lockmode', '1');
                if (ctx.cfg.demandLogLevel !== 'off') {
                    this.log.info(`Demand control: lockmode activated for "${ctx.folder}"`);
                }
            }
        }
        else {
            this.log.warn('MQTT disconnected');
            for (const folder of this.stateManagers.keys()) {
                try {
                    await this.setStateAsync(`${folder}.info.connection`, { val: false, ack: true });
                }
                catch {
                }
            }
        }
    }
    onStateChange(id, state) {
        {
            const foldersOd = this.outdoorIdToFolders.get(id);
            if (state?.ack && foldersOd) {
                for (const f of foldersOd) {
                    const ctx = this.demandCtxMap.get(f);
                    if (ctx) {
                        ctx.lastEventTs = Date.now();
                        void this._runDemandControl(f).catch((e) => this.log.error(`Demand control error: ${e}`));
                    }
                }
                return;
            }
            const foldersRm = this.roomIdToFolders.get(id);
            if (state?.ack && foldersRm) {
                for (const f of foldersRm) {
                    const ctx = this.demandCtxMap.get(f);
                    if (ctx) {
                        ctx.lastEventTs = Date.now();
                        void this._runDemandControl(f).catch((e) => this.log.error(`Demand control error: ${e}`));
                    }
                }
                return;
            }
        }
        if (state?.ack && this.demandCtxMap.size > 0) {
            for (const ctx of this.demandCtxMap.values()) {
                const pfx = `${this.namespace}.${ctx.folder}`;
                if (id === `${pfx}.power` || id === `${pfx}.mode`) {
                    ctx.lastEventTs = Date.now();
                    void this._runDemandControl(ctx.folder).catch((e) => this.log.error(`Demand control error: ${e}`));
                    return;
                }
            }
        }
        if (state && !state.ack && this.demandCtxMap.size > 0) {
            for (const ctx of this.demandCtxMap.values()) {
                const dp = `${this.namespace}.${ctx.folder}.demand_ctrl.`;
                if (id.startsWith(dp)) {
                    void (async () => {
                        await this.setStateAsync(id.slice(`${this.namespace}.`.length), { val: state.val, ack: true });
                        void this._runDemandControl(ctx.folder).catch((e) => this.log.error(`Demand control error: ${e}`));
                    })().catch((e) => this.log.error(`Demand override error: ${e}`));
                    return;
                }
            }
        }
        if (state && !state.ack) {
            for (const [folder] of this.humidityStates) {
                if (id === `${this.namespace}.${folder}.humidity_ctrl.auto`) {
                    void (async () => {
                        await this.setStateAsync(`${folder}.humidity_ctrl.auto`, { val: state.val, ack: true });
                        void this._runHumidityControl(folder).catch((e) => this.log.error(`Humidity control error: ${e}`));
                    })().catch((e) => this.log.error(`Humidity auto error: ${e}`));
                    return;
                }
            }
        }
        if (state?.ack && this.humidityStates.size > 0) {
            for (const [folder, st] of this.humidityStates) {
                const ns = `${this.namespace}.`;
                const tempId = (st.cfg.humTempStateId || '').trim() || `${ns}${folder}.ble.temp`;
                const humId = (st.cfg.humHumStateId || '').trim() || `${ns}${folder}.ble.hum`;
                if (id === tempId || id === humId) {
                    void this._runHumidityControl(folder).catch((e) => this.log.error(`Humidity control error: ${e}`));
                    break;
                }
            }
        }
        if (!state || state.ack) {
            return;
        }
        void this._handleStateWrite(id, state.val);
    }
    _handleStateWrite(id, val) {
        if (!this.mqttClient) {
            return;
        }
        const prefix = `${this.namespace}.`;
        if (!id.startsWith(prefix)) {
            return;
        }
        const relative = id.slice(prefix.length);
        const parts = relative.split('.');
        if (parts.length < 2) {
            return;
        }
        const folder = parts[0];
        const topics = this.folderToTopics.get(folder);
        if (!topics) {
            this.log.debug(`No topic info for folder "${folder}" – run Discovery first`);
            return;
        }
        if (parts[1] === 'config' && parts.length === 3) {
            const configField = parts[2];
            if (!fieldDefinitions_1.CONFIG_FIELDS_DEF[configField]) {
                return;
            }
            const topic = topics.settingTopic(configField);
            const payload = String(val ?? '');
            this.mqttClient.publish(topic, payload);
            this.log.debug(`Config write: ${topic} = ${payload}`);
            return;
        }
        if (parts.length !== 2) {
            return;
        }
        const fieldName = parts[1];
        const def = fieldDefinitions_1.FIELDS[fieldName];
        if (!def?.writeTopic) {
            return;
        }
        const payload = this._buildWritePayload(fieldName, def, val);
        if (payload === null) {
            return;
        }
        let topic;
        if (def.writeTopic.startsWith('setting:')) {
            const settingField = def.writeTopic.split(':')[1];
            topic = topics.settingTopic(settingField);
        }
        else if (def.writeTopic === 'setting') {
            topic = topics.settingJsonTopic;
        }
        else {
            topic = topics.commandTopic(def.writeTopic);
        }
        this.mqttClient.publish(topic, payload);
        this.log.debug(`State write: ${topic} = ${payload}`);
    }
    _buildWritePayload(key, def, val) {
        if (val === null || val === undefined) {
            return null;
        }
        if (def.writeTopic === 'control') {
            const jsonKey = key === 'env_set' ? 'env' : key;
            return JSON.stringify({ [jsonKey]: val });
        }
        if (def.writeTopic === 'setting') {
            return JSON.stringify({ [key]: String(val ?? '').replace(':', '') });
        }
        if (def.writeTopic?.startsWith('setting:')) {
            return String(val);
        }
        if (def.type === 'boolean') {
            return val ? '1' : '0';
        }
        return String(val);
    }
    onMessage(obj) {
        if (obj.command === 'discovery') {
            const msg = obj.message;
            const name = msg?.name;
            if (!name) {
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, { error: 'No device name provided' }, obj.callback);
                }
                return;
            }
            const devices = this.config.devices ?? [];
            const device = devices.find((d) => d.name === name);
            if (!device?.ip) {
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, { error: `Device "${name}" not found in configuration` }, obj.callback);
                }
                return;
            }
            const ip = device.ip;
            this.log.info(`Discovery triggered for "${name}" at ${ip}`);
            (0, deviceDiscovery_1.discoverDevice)(ip)
                .then(async (profile) => {
                const folder = name.replace(/[^a-zA-Z0-9_-]/g, '_');
                let sm = this.stateManagers.get(folder);
                if (!sm) {
                    sm = new stateManager_1.StateManager(this, folder);
                    this.stateManagers.set(folder, sm);
                }
                await sm.createStates(profile, true);
                await this.setStateAsync(`${folder}.info.deviceId`, { val: profile.deviceId, ack: true });
                await this.setStateAsync(`${folder}.info.appName`, { val: profile.appName, ack: true });
                await this.setStateAsync(`${folder}.info.ip`, { val: ip, ack: true });
                const topics = (0, topicBuilder_1.buildTopicPaths)(profile.deviceId, profile.appName, profile.topicConfig);
                if (!this.topicToFolder.has(topics.statusTopic)) {
                    this.topicToFolder.set(topics.statusTopic, folder);
                    if (this.mqttClient) {
                        this.mqttClient.subscribe(topics.statusTopic);
                        this.log.info(`Subscribed to: ${topics.statusTopic}`);
                    }
                }
                this.folderToTopics.set(folder, topics);
                await this.setStateAsync(`${folder}.config.livestatus`, { val: true, ack: false });
                this.log.info(`Discovery completed for "${name}": ${profile.deviceId} (${profile.appName}) – ${profile.activeFields.size} fields`);
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, {
                        result: `Discovered: ${profile.deviceId} (${profile.appName || 'unknown'}) – ${profile.activeFields.size} fields`,
                    }, obj.callback);
                }
            })
                .catch((err) => {
                this.log.error(`Discovery failed for "${name}" (${ip}): ${err.message}`);
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, { error: err.message }, obj.callback);
                }
            });
        }
        if (obj.command === 'getDeviceList') {
            const devices = this.config.devices ?? [];
            if (obj.callback) {
                this.sendTo(obj.from, obj.command, devices.map((d) => ({ label: d.name, value: d.name })), obj.callback);
            }
        }
        if (obj.command === 'deleteDevice') {
            const msg = obj.message;
            const name = msg?.name;
            if (!name) {
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, { error: 'No device name provided' }, obj.callback);
                }
                return;
            }
            const folder = name.replace(/[^a-zA-Z0-9_-]/g, '_');
            this.log.info(`Deleting all states for device "${name}" (${folder})`);
            void (async () => {
                try {
                    const allObjs = await this.getObjectListAsync({
                        startkey: `${this.namespace}.${folder}.`,
                        endkey: `${this.namespace}.${folder}.\u9999`,
                    });
                    const withCustom = (allObjs?.rows ?? []).filter((r) => {
                        const custom = r.value?.common?.custom;
                        return custom && Object.keys(custom).length > 0;
                    });
                    if (withCustom.length > 0) {
                        const ids = withCustom.map((r) => r.id).join(', ');
                        this.log.warn(`Device "${name}": ${withCustom.length} state(s) have custom data (History/InfluxDB) – deletion skipped to preserve it. Affected: ${ids}`);
                        if (obj.callback) {
                            this.sendTo(obj.from, obj.command, {
                                error: `Cannot delete: ${withCustom.length} state(s) have History/InfluxDB custom data. Remove the custom data first.`,
                            }, obj.callback);
                        }
                        return;
                    }
                    await this.delObjectAsync(folder, { recursive: true });
                    this.stateManagers.delete(folder);
                    const topicsToRemove = [...this.topicToFolder.entries()]
                        .filter(([, f]) => f === folder)
                        .map(([t]) => t);
                    for (const topic of topicsToRemove) {
                        this.topicToFolder.delete(topic);
                        this.mqttClient?.unsubscribe(topic);
                    }
                    this.folderToTopics.delete(folder);
                    this.log.info(`Deleted all states for device "${name}"`);
                    if (obj.callback) {
                        this.sendTo(obj.from, obj.command, { result: `States for "${name}" deleted` }, obj.callback);
                    }
                }
                catch (err) {
                    this.log.error(`Failed to delete states for "${name}": ${String(err)}`);
                    if (obj.callback) {
                        this.sendTo(obj.from, obj.command, { error: String(err) }, obj.callback);
                    }
                }
            })();
        }
    }
    _setupSchedule() {
        const cfg = this.config;
        const entries = cfg.schedule || [];
        if (entries.length === 0) {
            return;
        }
        const sceneModes = cfg.sceneModes || [];
        this.setInterval(async () => {
            const now = new Date();
            const dow = (now.getDay() + 6) % 7;
            const hhmm = now.toTimeString().slice(0, 5);
            const dateKey = now.toISOString().slice(0, 10);
            const firedKey = `${dateKey} ${hhmm}`;
            for (let i = 0; i < entries.length; i++) {
                const entry = entries[i];
                if (!entry.enabled) {
                    continue;
                }
                const entryScene = (entry.scene || '').trim();
                if (entryScene) {
                    const mode = sceneModes.find((m) => m.name === entryScene);
                    if (!mode || !mode.active) {
                        continue;
                    }
                }
                const entryTime = (entry.time || '').trim();
                const entryDays = entry.days || [];
                if (entryTime !== hhmm) {
                    continue;
                }
                if (!entryDays.includes(dow)) {
                    continue;
                }
                if (this.scheduleFired.get(i) === firedKey) {
                    continue;
                }
                const deviceName = (entry.deviceName || '').trim();
                const actions = Array.isArray(entry.actions)
                    ? entry.actions
                    : entry.field
                        ? [{ field: entry.field, value: entry.value }]
                        : [];
                if (!deviceName || actions.length === 0) {
                    continue;
                }
                const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
                let fired = false;
                for (const action of actions) {
                    const field = (action.field || '').trim();
                    const value = action.value;
                    if (!field) {
                        continue;
                    }
                    try {
                        if (field === 'demand_override_on') {
                            const overrideVal = Math.max(30, Math.min(100, Math.round(Number(value ?? 50))));
                            await this.setStateAsync(`${folder}.demand_ctrl.override_value`, {
                                val: overrideVal,
                                ack: false,
                            });
                            await this.setStateAsync(`${folder}.demand_ctrl.override_active`, {
                                val: true,
                                ack: false,
                            });
                            this.log.info(`Schedule [${deviceName}] demand override ON = ${overrideVal}%`);
                        }
                        else if (field === 'demand_override_off') {
                            await this.setStateAsync(`${folder}.demand_ctrl.override_active`, {
                                val: false,
                                ack: false,
                            });
                            this.log.info(`Schedule [${deviceName}] demand override OFF`);
                        }
                        else {
                            await this.setStateAsync(`${folder}.${field}`, {
                                val: value,
                                ack: false,
                            });
                            this.log.info(`Schedule [${deviceName}] ${field}=${JSON.stringify(value)}`);
                        }
                        fired = true;
                    }
                    catch (err) {
                        this.log.warn(`Schedule [${deviceName}] ${field} failed: ${String(err)}`);
                    }
                    const delayMs = Math.max(0, Number(action.delay ?? 0)) * 1000;
                    if (delayMs > 0) {
                        await new Promise((resolve) => this.setTimeout(resolve, delayMs));
                    }
                }
                if (fired) {
                    this.scheduleFired.set(i, firedKey);
                }
            }
        }, 30_000);
    }
    _setupEnvSensors() {
        const cfg = this.config;
        const entries = cfg.envSensors || [];
        for (const entry of entries) {
            const deviceName = (entry.deviceName || '').trim();
            const sensorStateId = (entry.sensorStateId || '').trim();
            const intervalSec = Math.min(Math.max(10, Number(entry.intervalSec ?? 30)), 2_147_483);
            if (!deviceName || !sensorStateId) {
                continue;
            }
            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const targetId = `${this.namespace}.${folder}.env_set`;
            const publish = async () => {
                try {
                    const state = await this.getStateAsync(sensorStateId);
                    if (!state || state.val === null || state.val === undefined) {
                        return;
                    }
                    const val = Number(state.val);
                    if (isNaN(val)) {
                        return;
                    }
                    await this.setStateAsync(targetId, { val, ack: false });
                    this.log.debug(`EnvSensor [${deviceName}] env_set=${val}`);
                }
                catch {
                }
            };
            void publish();
            this.setInterval(publish, intervalSec * 1_000);
            this.log.info(`EnvSensor [${deviceName}] → ${sensorStateId} every ${intervalSec}s`);
        }
    }
    async _setupDemandControl() {
        const cfg = this.config;
        const devices = cfg.devices || [];
        const demandDevices = devices.filter((d) => d.enabled !== false && d.useDemand);
        if (demandDevices.length === 0) {
            this.log.info('Demand control: no device has useDemand checkbox set – skipping');
            return;
        }
        const FLAT_SKIP = new Set(['demandOutdoorTempId', 'demandRoomTempId']);
        const flatDefaults = {};
        for (const [k, v] of Object.entries(cfg)) {
            if (k.startsWith('demand') && !FLAT_SKIP.has(k)) {
                flatDefaults[k] = v;
            }
        }
        for (const deviceEntry of demandDevices) {
            const deviceName = (deviceEntry.name || '').trim();
            if (!deviceName) {
                this.log.warn('Demand control: device name empty – skipping entry');
                continue;
            }
            const devCfg = Object.assign({}, flatDefaults, cfg[deviceName] || {});
            if (!devCfg.demandEnabled) {
                this.log.info(`Demand control: demandEnabled not set for "${deviceName}" – skipping`);
                continue;
            }
            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const group = (deviceEntry.group || '').trim();
            const outdoorId = (devCfg.demandOutdoorTempId || '').trim() || `${this.namespace}.${folder}.outside`;
            const roomId = (devCfg.demandRoomTempId || '').trim() || `${this.namespace}.${folder}.env`;
            const ctx = {
                folder,
                outdoorId,
                roomId,
                cfg: devCfg,
                group,
                guardActive: false,
                running: false,
                lastRunTs: 0,
                lastLogged: null,
                lastSent: null,
                lastChangeTs: 0,
                lastEventTs: 0,
            };
            this.demandCtxMap.set(folder, ctx);
            const od = this.outdoorIdToFolders.get(outdoorId) ?? [];
            if (!od.includes(folder)) {
                od.push(folder);
            }
            this.outdoorIdToFolders.set(outdoorId, od);
            const rm = this.roomIdToFolders.get(roomId) ?? [];
            if (!rm.includes(folder)) {
                rm.push(folder);
            }
            this.roomIdToFolders.set(roomId, rm);
            if (!outdoorId.startsWith(`${this.namespace}.`)) {
                await this.subscribeForeignStatesAsync(outdoorId);
            }
            if (!roomId.startsWith(`${this.namespace}.`)) {
                await this.subscribeForeignStatesAsync(roomId);
            }
            const dp = `${folder}.demand_ctrl`;
            await this.extendObjectAsync(dp, {
                type: 'channel',
                common: {
                    name: {
                        en: 'Demand Control',
                        de: 'Bedarfsregelung',
                        ru: 'Регулирование мощности',
                        pt: 'Controlo de demanda',
                        nl: 'Vraagregeling',
                        fr: 'Régulation de la demande',
                        it: 'Controllo domanda',
                        es: 'Control de demanda',
                        pl: 'Sterowanie zapotrzebowaniem',
                        uk: 'Регулювання попиту',
                        'zh-cn': '需求控制',
                    },
                },
                native: {},
            });
            const stBase = { type: 'state', native: {} };
            await this.extendObjectAsync(`${dp}.active`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Demand control active',
                        de: 'Bedarfsregelung aktiv',
                        ru: 'Регулирование мощности активно',
                        pt: 'Controlo de demanda ativo',
                        nl: 'Vraagregeling actief',
                        fr: 'Régulation de demande active',
                        it: 'Controllo domanda attivo',
                        es: 'Control de demanda activo',
                        pl: 'Sterowanie zapotrzebowaniem aktywne',
                        uk: 'Регулювання попиту активне',
                        'zh-cn': '需求控制已激活',
                    },
                    type: 'boolean',
                    role: 'indicator',
                    read: true,
                    write: false,
                    def: false,
                },
            });
            await this.extendObjectAsync(`${dp}.demand_calc`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Calculated demand',
                        de: 'Berechneter Bedarf',
                        ru: 'Расчётный спрос',
                        pt: 'Demanda calculada',
                        nl: 'Berekende vraag',
                        fr: 'Demande calculée',
                        it: 'Domanda calcolata',
                        es: 'Demanda calculada',
                        pl: 'Obliczone zapotrzebowanie',
                        uk: 'Розрахований попит',
                        'zh-cn': '计算需求',
                    },
                    type: 'number',
                    unit: '%',
                    role: 'value',
                    read: true,
                    write: false,
                    min: 0,
                    max: 100,
                },
            });
            await this.extendObjectAsync(`${dp}.override_active`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Manual override active',
                        de: 'Manuelle Übersteuerung aktiv',
                        ru: 'Ручное переопределение активно',
                        pt: 'Substituição manual ativa',
                        nl: 'Handmatige override actief',
                        fr: 'Remplacement manuel actif',
                        it: 'Override manuale attivo',
                        es: 'Anulación manual activa',
                        pl: 'Ręczne nadpisanie aktywne',
                        uk: 'Ручне перевизначення активне',
                        'zh-cn': '手动覆盖已激活',
                    },
                    type: 'boolean',
                    role: 'switch',
                    read: true,
                    write: true,
                    def: false,
                },
            });
            await this.extendObjectAsync(`${dp}.override_value`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Manual override demand',
                        de: 'Manueller Bedarfswert',
                        ru: 'Значение ручного переопределения',
                        pt: 'Valor de substituição manual',
                        nl: 'Handmatige override waarde',
                        fr: 'Valeur de remplacement manuel',
                        it: 'Valore override manuale',
                        es: 'Valor de anulación manual',
                        pl: 'Wartość ręcznego nadpisania',
                        uk: 'Значення ручного перевизначення',
                        'zh-cn': '手动覆盖需求值',
                    },
                    type: 'number',
                    unit: '%',
                    role: 'level',
                    read: true,
                    write: true,
                    min: 0,
                    max: 100,
                    def: 50,
                },
            });
            await this.extendObjectAsync(`${dp}.guard_active`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Guard active',
                        de: 'Guard aktiv',
                        ru: 'Защита активна',
                        pt: 'Guarda ativo',
                        nl: 'Guard actief',
                        fr: 'Protection active',
                        it: 'Guard attivo',
                        es: 'Guard activo',
                        pl: 'Guard aktywny',
                        uk: 'Guard активний',
                        'zh-cn': 'Guard 已激活',
                    },
                    type: 'boolean',
                    role: 'indicator',
                    read: true,
                    write: false,
                    def: false,
                },
            });
            await this.extendObjectAsync(`${dp}.outdoor_temp`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Outdoor temperature used',
                        de: 'Verwendete Außentemperatur',
                        ru: 'Используемая уличная температура',
                        pt: 'Temperatura exterior utilizada',
                        nl: 'Gebruikte buitentemperatuur',
                        fr: 'Température extérieure utilisée',
                        it: 'Temperatura esterna utilizzata',
                        es: 'Temperatura exterior utilizada',
                        pl: 'Użyta temperatura zewnętrzna',
                        uk: 'Використана вулична температура',
                        'zh-cn': '使用的室外温度',
                    },
                    type: 'number',
                    unit: '°C',
                    role: 'value.temperature',
                    read: true,
                    write: false,
                },
            });
            await this.extendObjectAsync(`${dp}.room_temp`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Room temperature used',
                        de: 'Verwendete Raumtemperatur',
                        ru: 'Используемая комнатная температура',
                        pt: 'Temperatura interior utilizada',
                        nl: 'Gebruikte kamertemperatuur',
                        fr: 'Température ambiante utilisée',
                        it: 'Temperatura ambiente utilizzata',
                        es: 'Temperatura interior utilizada',
                        pl: 'Użyta temperatura pokojowa',
                        uk: 'Використана кімнатна температура',
                        'zh-cn': '使用的室温',
                    },
                    type: 'number',
                    unit: '°C',
                    role: 'value.temperature',
                    read: true,
                    write: false,
                },
            });
            await this.setStateAsync(`${dp}.active`, { val: true, ack: true });
            this.setInterval(() => {
                if (Date.now() - ctx.lastEventTs >= 180_000) {
                    void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
                }
            }, 180_000);
            this.log.info(`Demand control active for "${deviceName}" | outdoor: ${outdoorId} | room: ${roomId}`);
            void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
        }
    }
    async _runDemandControl(folder) {
        const ctx = this.demandCtxMap.get(folder);
        if (!ctx) {
            return;
        }
        if (ctx.running) {
            return;
        }
        const now = Date.now();
        if (now - ctx.lastRunTs < 60_000) {
            return;
        }
        ctx.running = true;
        ctx.lastRunTs = now;
        try {
            await this._runDemandControlCore(ctx);
        }
        finally {
            ctx.running = false;
        }
    }
    async _runDemandControlCore(ctx) {
        const { cfg, folder, outdoorId, roomId } = ctx;
        const ns = `${this.namespace}.`;
        const outdoorState = outdoorId.startsWith(ns)
            ? await this.getStateAsync(outdoorId.slice(ns.length))
            : await this.getForeignStateAsync(outdoorId);
        const roomState = roomId.startsWith(ns)
            ? await this.getStateAsync(roomId.slice(ns.length))
            : await this.getForeignStateAsync(roomId);
        if (outdoorState?.val == null || roomState?.val == null) {
            return;
        }
        const outdoor = Number(outdoorState.val);
        const room = Number(roomState.val);
        const dp = `${folder}.demand_ctrl`;
        await this.setStateAsync(`${dp}.outdoor_temp`, { val: outdoor, ack: true });
        await this.setStateAsync(`${dp}.room_temp`, { val: room, ack: true });
        const powerState = await this.getStateAsync(`${folder}.power`);
        const modeState = await this.getStateAsync(`${folder}.mode`);
        const isActive = powerState?.val === true && modeState?.val === 'H';
        const curve = cfg.demandCurve ?? [];
        if (curve.length < 2) {
            this.log.warn('Demand curve has fewer than 2 support points – skipping');
            return;
        }
        let demand = this._interpolateCurve(curve, outdoor);
        const baseDemand = demand;
        let boostApplied = 0;
        let boostStageDesc = '';
        if (cfg.demandBoostEnabled) {
            const autotState = await this.getStateAsync(`${folder}.autot`);
            if (autotState?.val != null) {
                const autot = Number(autotState.val);
                const diff = autot - room;
                const stages = cfg.demandBoostStages ?? [];
                const sortedStages = [...stages].sort((a, b) => Number(b.deviation) - Number(a.deviation));
                for (const stage of sortedStages) {
                    if (diff >= Number(stage.deviation)) {
                        boostApplied = Number(stage.boost);
                        boostStageDesc = `≥${stage.deviation}K`;
                        demand = Math.min(Number(cfg.demandMaxDemand ?? 95), demand + boostApplied);
                        break;
                    }
                }
            }
        }
        let taperApplied = 0;
        if (cfg.demandTaperEnabled) {
            const autotState = await this.getStateAsync(`${folder}.autot`);
            const autorState = await this.getStateAsync(`${folder}.autor`);
            if (autotState?.val != null) {
                const autot = Number(autotState.val);
                const autor = autorState?.val != null ? Number(autorState.val) : 0.3;
                const guardThreshold = autot + autor + 0.1;
                if (room > autot && room < guardThreshold) {
                    const taperMax = Number(cfg.demandTaperMax ?? 15);
                    const taperFraction = (room - autot) / (guardThreshold - autot);
                    taperApplied = taperFraction * taperMax;
                    demand = Math.max(Number(cfg.demandMinDemand ?? 30), demand - taperApplied);
                }
            }
        }
        await this._checkGuard(ctx, room);
        demand = Math.min(Number(cfg.demandMaxDemand ?? 95), demand);
        demand = Math.max(Number(cfg.demandMinDemand ?? 30), demand);
        demand = Math.round(demand);
        if (ctx.group) {
            let groupFloor = demand;
            for (const peer of this.demandCtxMap.values()) {
                if (peer.folder !== folder && peer.group === ctx.group) {
                    groupFloor = Math.max(groupFloor, Number(peer.cfg.demandMinDemand ?? 30));
                }
            }
            demand = Math.max(demand, groupFloor);
        }
        await this.setStateAsync(`${dp}.guard_active`, { val: ctx.guardActive, ack: true });
        await this.setStateAsync(`${dp}.demand_calc`, { val: demand, ack: true });
        if (!isActive) {
            this.log.debug(`Demand control skipped: power=${String(powerState?.val)} mode=${String(modeState?.val ?? '?')}`);
            return;
        }
        const overrideActive = (await this.getStateAsync(`${dp}.override_active`))?.val === true;
        if (overrideActive) {
            const overrideDemand = Math.round(Number((await this.getStateAsync(`${dp}.override_value`))?.val ?? 50));
            await this.setStateAsync(`${dp}.demand_calc`, { val: overrideDemand, ack: true });
            this._sendDemand(ctx, overrideDemand);
            this.log.debug(`Demand override: ${overrideDemand}%`);
            return;
        }
        if (cfg.demandLogLevel === 'extended') {
            const boostStr = boostApplied > 0 ? `+${boostApplied}%(${boostStageDesc})` : '–';
            const taperStr = taperApplied > 0 ? `-${taperApplied.toFixed(1)}%` : '–';
            const guardStr = ctx.guardActive ? 'ON' : 'OFF';
            this.log.info(`Demand [${folder}] outdoor=${outdoor}°C room=${room}°C` +
                ` | base=${baseDemand.toFixed(0)}% boost=${boostStr} taper=${taperStr}` +
                ` → target=${demand}% | guard=${guardStr}`);
        }
        else if (demand !== ctx.lastLogged) {
            this.log.info(`Demand [${folder}] outdoor=${outdoor}°C room=${room}°C → target=${demand}%`);
            ctx.lastLogged = demand;
        }
        this._scheduleDemand(ctx, demand);
    }
    _scheduleDemand(ctx, target) {
        const { cfg, folder } = ctx;
        const delayDown = Number(cfg.demandDelayReduce ?? 0) * 1000;
        const delayUp = Number(cfg.demandDelayIncrease ?? 300) * 1000;
        if (ctx.lastSent === null) {
            this._sendDemand(ctx, target);
            return;
        }
        if (target === ctx.lastSent) {
            return;
        }
        const goingUp = target > ctx.lastSent;
        const delay = goingUp ? delayUp : delayDown;
        const elapsed = Date.now() - ctx.lastChangeTs;
        if (delay <= 0 || elapsed >= delay) {
            this._sendDemand(ctx, target);
        }
        else {
            this.log.info(`Demand [${folder}] ${goingUp ? '↑' : '↓'} ${ctx.lastSent}%→${target}% gehalten, noch ${Math.round((delay - elapsed) / 1000)}s`);
        }
    }
    _sendDemand(ctx, demand) {
        ctx.lastSent = demand;
        ctx.lastChangeTs = Date.now();
        void this.setStateAsync(`${ctx.folder}.demand`, { val: demand, ack: false }).catch(() => {
            this.log.warn(`Demand state not found for "${ctx.folder}" – run Discovery first`);
        });
        this.log.debug(`Demand [${ctx.folder}] sent=${demand}%`);
    }
    async _checkGuard(ctx, room) {
        const { cfg, folder } = ctx;
        if (!cfg.demandGuardEnabled) {
            return;
        }
        const autotState = await this.getStateAsync(`${folder}.autot`);
        const autorState = await this.getStateAsync(`${folder}.autor`);
        if (autotState?.val == null) {
            return;
        }
        const autot = Number(autotState.val);
        const autor = autorState?.val != null ? Number(autorState.val) : 0.3;
        const guardThreshold = autot + autor + 0.1;
        const heatbackNormal = Number(cfg.demandHeatbackNormal ?? 3);
        const heatbackGuard = Number(cfg.demandHeatbackGuard ?? 6);
        const logOff = cfg.demandLogLevel === 'off';
        if (room >= guardThreshold && !ctx.guardActive) {
            ctx.guardActive = true;
            this._publishGuardHeatback(folder, heatbackGuard);
            if (!logOff) {
                this.log.info(`Demand guard ON: room=${room}°C >= threshold=${guardThreshold.toFixed(2)}°C → heatback=${heatbackGuard}`);
            }
        }
        else if (room <= autot && ctx.guardActive) {
            ctx.guardActive = false;
            this._publishGuardHeatback(folder, heatbackNormal);
            if (!logOff) {
                this.log.info(`Demand guard OFF: room=${room}°C <= autot=${autot}°C → heatback=${heatbackNormal}`);
            }
        }
    }
    _interpolateCurve(curve, x) {
        const sorted = [...curve].sort((a, b) => Number(a.temp) - Number(b.temp));
        if (x <= Number(sorted[0].temp)) {
            return Number(sorted[0].demand);
        }
        if (x >= Number(sorted[sorted.length - 1].temp)) {
            return Number(sorted[sorted.length - 1].demand);
        }
        for (let i = 0; i < sorted.length - 1; i++) {
            const t0 = Number(sorted[i].temp), t1 = Number(sorted[i + 1].temp);
            if (x >= t0 && x <= t1) {
                const frac = (x - t0) / (t1 - t0);
                return Number(sorted[i].demand) + frac * (Number(sorted[i + 1].demand) - Number(sorted[i].demand));
            }
        }
        return 0;
    }
    _publishGuardHeatback(folder, value) {
        this._publishDemandConfigParam(folder, 'heatback', String(value));
        this.log.debug(`Guard heatback → ${value}`);
    }
    _publishDemandConfigParam(folder, param, value) {
        const topics = this.folderToTopics.get(folder);
        if (!topics || !this.mqttClient) {
            return;
        }
        this.mqttClient.publish(topics.settingTopic(param), value);
    }
    async _setupHumidityControls() {
        const cfg = this.config;
        const devices = cfg.devices || [];
        const humCfg = cfg.humCfg ?? {};
        const humDevices = devices.filter((d) => d.enabled !== false && d.useHumidity);
        if (humDevices.length === 0) {
            return;
        }
        for (const device of humDevices) {
            const deviceName = (device.name || '').trim();
            if (!deviceName) {
                continue;
            }
            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const dp = `${folder}.humidity_ctrl`;
            const devCfg = Object.assign({
                humFreshMin: 20,
                humTpHoch: 4.0,
                humTpTief: 7.0,
                humMaxFeuchte: 75,
                humMaxLaufzeit: 240,
                humPauseMin: 30,
                humNachlaufMin: 15,
                humStartZeit: '07:00',
                humEndZeit: '22:00',
                humTempStateId: '',
                humHumStateId: '',
            }, humCfg[deviceName] ?? {});
            const stBase = { type: 'state', native: {} };
            await this.extendObjectAsync(dp, {
                type: 'channel',
                common: {
                    name: {
                        en: 'Humidity Control',
                        de: 'Feuchtesteuerung',
                        ru: 'Управление влажностью',
                        pt: 'Controlo de humidade',
                        nl: 'Vochtigheidsbeheer',
                        fr: "Régulation de l'humidité",
                        it: 'Controllo umidità',
                        es: 'Control de humedad',
                        pl: 'Sterowanie wilgotnością',
                        uk: 'Керування вологістю',
                        'zh-cn': '湿度控制',
                    },
                },
                native: {},
            });
            await this.extendObjectAsync(`${dp}.auto`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Humidity auto',
                        de: 'Feuchte-Automatik',
                        ru: 'Авто-режим влажности',
                        pt: 'Humidade automática',
                        nl: 'Vochtigheid automatisch',
                        fr: 'Humidité auto',
                        it: 'Umidità automatica',
                        es: 'Humedad automática',
                        pl: 'Wilgotność auto',
                        uk: 'Авторежим вологості',
                        'zh-cn': '湿度自动',
                    },
                    type: 'boolean',
                    role: 'switch',
                    read: true,
                    write: true,
                    def: false,
                },
            });
            await this.extendObjectAsync(`${dp}.mode`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Humidity mode',
                        de: 'Feuchte-Modus',
                        ru: 'Режим влажности',
                        pt: 'Modo de humidade',
                        nl: 'Vochtigheidsmodus',
                        fr: 'Mode humidité',
                        it: 'Modalità umidità',
                        es: 'Modo humedad',
                        pl: 'Tryb wilgotności',
                        uk: 'Режим вологості',
                        'zh-cn': '湿度模式',
                    },
                    type: 'number',
                    role: 'value',
                    read: true,
                    write: false,
                    def: 0,
                    states: { 0: 'OFF', 1: 'MONITORING', 2: 'DRY', 3: 'RUN-ON', 4: 'PAUSE', 5: 'FRESHNESS!' },
                },
            });
            await this.extendObjectAsync(`${dp}.mode_text`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Humidity mode text',
                        de: 'Feuchte-Modus Text',
                        ru: 'Текст режима влажности',
                        pt: 'Texto do modo de humidade',
                        nl: 'Vochtigheidsmodus tekst',
                        fr: 'Texte du mode humidité',
                        it: 'Testo modalità umidità',
                        es: 'Texto de modo humedad',
                        pl: 'Tekst trybu wilgotności',
                        uk: 'Текст режиму вологості',
                        'zh-cn': '湿度模式文本',
                    },
                    type: 'string',
                    role: 'text',
                    read: true,
                    write: false,
                    def: 'OFF',
                },
            });
            await this.extendObjectAsync(`${dp}.dew_point`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Dew point',
                        de: 'Taupunkt',
                        ru: 'Точка росы',
                        pt: 'Ponto de orvalho',
                        nl: 'Dauwpunt',
                        fr: 'Point de rosée',
                        it: 'Punto di rugiada',
                        es: 'Punto de rocío',
                        pl: 'Punkt rosy',
                        uk: 'Точка роси',
                        'zh-cn': '露点',
                    },
                    type: 'number',
                    unit: '°C',
                    role: 'value.temperature',
                    read: true,
                    write: false,
                },
            });
            await this.extendObjectAsync(`${dp}.dew_point_diff`, {
                ...stBase,
                common: {
                    name: {
                        en: 'Dew point distance',
                        de: 'Taupunkt-Abstand',
                        ru: 'Расстояние до точки росы',
                        pt: 'Distância ao ponto de orvalho',
                        nl: 'Dauwpuntafstand',
                        fr: 'Distance au point de rosée',
                        it: 'Distanza dal punto di rugiada',
                        es: 'Distancia al punto de rocío',
                        pl: 'Odległość od punktu rosy',
                        uk: 'Відстань до точки роси',
                        'zh-cn': '露点距离',
                    },
                    type: 'number',
                    unit: 'K',
                    role: 'value',
                    read: true,
                    write: false,
                },
            });
            this.humidityStates.set(folder, {
                cfg: devCfg,
                mode: 0,
                dryStart: 0,
                nachlaufStart: 0,
                pauseEnd: 0,
                afterNachlauf: 'ueberwachung',
                lastCmdTs: 0,
                running: false,
            });
            this.log.info(`Humidity control active for "${deviceName}"`);
            this.setInterval(() => {
                void this._runHumidityControl(folder).catch((e) => this.log.error(`Humidity control error: ${e}`));
            }, 30_000);
            void this._runHumidityControl(folder).catch((e) => this.log.error(`Humidity control error: ${e}`));
        }
    }
    async _runHumidityControl(folder) {
        const st = this.humidityStates.get(folder);
        if (!st || st.running) {
            return;
        }
        st.running = true;
        try {
            await this._runHumidityControlCore(folder, st);
        }
        finally {
            st.running = false;
        }
    }
    async _runHumidityControlCore(folder, st) {
        const MODI = { AUS: 0, UE: 1, DRY: 2, NL: 3, ZP: 4, FF: 5 };
        const MOD_NAME = ['OFF', 'MONITORING', 'DRY', 'RUN-ON', 'PAUSE', 'FRESHNESS!'];
        const cfg = st.cfg;
        const dp = `${folder}.humidity_ctrl`;
        const ns = `${this.namespace}.`;
        const toMs = (m) => m * 60_000;
        const now = Date.now();
        const setMode = async (m) => {
            st.mode = m;
            await this.setStateAsync(`${dp}.mode`, { val: m, ack: true });
            await this.setStateAsync(`${dp}.mode_text`, { val: MOD_NAME[m] ?? '?', ack: true });
        };
        const autoState = await this.getStateAsync(`${dp}.auto`);
        const auto = autoState?.val === true;
        const tempId = (cfg.humTempStateId || '').trim() || `${ns}${folder}.ble.temp`;
        const humId = (cfg.humHumStateId || '').trim() || `${ns}${folder}.ble.hum`;
        const tempState = tempId.startsWith(ns)
            ? await this.getStateAsync(tempId.slice(ns.length))
            : await this.getForeignStateAsync(tempId);
        const humState = humId.startsWith(ns)
            ? await this.getStateAsync(humId.slice(ns.length))
            : await this.getForeignStateAsync(humId);
        const bleTemp = tempState?.val != null && !isNaN(Number(tempState.val)) ? Number(tempState.val) : undefined;
        const bleHum = humState?.val != null && !isNaN(Number(humState.val)) ? Number(humState.val) : undefined;
        const bleTs = Math.min(tempState?.ts ?? 0, humState?.ts ?? 0);
        const freshMin = Math.max(15, Number(cfg.humFreshMin ?? 20));
        const stale = bleTs === 0 || now - bleTs > toMs(freshMin);
        const startNachlauf = async (reason) => {
            st.nachlaufStart = now;
            st.dryStart = 0;
            st.afterNachlauf = reason;
            await this._humCmd(folder, true, 'F', 'A');
        };
        if (st.nachlaufStart > 0 && now - st.nachlaufStart >= toMs(Number(cfg.humNachlaufMin ?? 15))) {
            await this._humCmd(folder, false);
            st.nachlaufStart = 0;
            const reason = st.afterNachlauf;
            st.afterNachlauf = 'ueberwachung';
            if (st.mode === MODI.FF) {
                await this.setStateAsync(`${dp}.auto`, { val: false, ack: true });
                await setMode(MODI.AUS);
            }
            else if (reason === 'zwangspause') {
                st.pauseEnd = now + toMs(Number(cfg.humPauseMin ?? 30));
                await setMode(MODI.ZP);
            }
            else {
                await setMode(auto ? MODI.UE : MODI.AUS);
            }
            return;
        }
        if (st.mode === MODI.ZP && st.pauseEnd > 0 && now >= st.pauseEnd) {
            st.pauseEnd = 0;
            await setMode(MODI.UE);
        }
        if (!auto) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('ueberwachung');
                await setMode(MODI.NL);
            }
            else if (![MODI.NL, MODI.ZP, MODI.FF].includes(st.mode)) {
                if (st.mode !== MODI.AUS) {
                    await this._humCmd(folder, false);
                    await setMode(MODI.AUS);
                }
            }
            return;
        }
        if (stale && st.mode !== MODI.FF) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('freshness');
                await setMode(MODI.NL);
            }
            await setMode(MODI.FF);
            this.log.warn(`Humidity [${folder}] sensor stale`);
            return;
        }
        if ([MODI.FF, MODI.NL, MODI.ZP].includes(st.mode)) {
            return;
        }
        if (bleTemp === undefined || bleHum === undefined) {
            if (st.mode === MODI.AUS) {
                await this.setStateAsync(`${dp}.auto`, { val: false, ack: true });
            }
            return;
        }
        if (st.mode === MODI.AUS) {
            await setMode(MODI.UE);
        }
        const tp = this._taupunkt(bleTemp, bleHum);
        const diff = bleTemp - tp;
        await this.setStateAsync(`${dp}.dew_point`, { val: Math.round(tp * 10) / 10, ack: true });
        await this.setStateAsync(`${dp}.dew_point_diff`, { val: Math.round(diff * 10) / 10, ack: true });
        this.log.debug(`Humidity [${folder}] T=${bleTemp}°C rH=${bleHum}% TP=${tp.toFixed(1)}° Δ=${diff.toFixed(1)}K mode=${MOD_NAME[st.mode]}`);
        if (!this._humZeitErlaubt(cfg.humStartZeit || '07:00', cfg.humEndZeit || '22:00')) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('ueberwachung');
                await setMode(MODI.NL);
            }
            return;
        }
        if (st.mode === MODI.DRY && st.dryStart > 0 && now - st.dryStart >= toMs(Number(cfg.humMaxLaufzeit ?? 240))) {
            this.log.info(`Humidity [${folder}] max runtime – forced pause`);
            await startNachlauf('zwangspause');
            await setMode(MODI.NL);
            return;
        }
        if (st.mode === MODI.DRY && now - st.lastCmdTs > 120_000) {
            const realMode = await this.getStateAsync(`${folder}.mode`);
            const realPower = await this.getStateAsync(`${folder}.power`);
            if (realPower?.val !== false && realMode?.val !== 'D') {
                this.log.warn(`Humidity [${folder}] override – going to RUN-ON`);
                await startNachlauf('ueberwachung');
                await setMode(MODI.NL);
                return;
            }
        }
        const tpHoch = Number(cfg.humTpHoch ?? 4.0);
        const tpTief = Number(cfg.humTpTief ?? 7.0);
        const maxFeucht = Number(cfg.humMaxFeuchte ?? 75);
        if (st.mode === MODI.UE && (diff <= tpHoch || bleHum >= maxFeucht)) {
            this.log.info(`Humidity [${folder}] start DRY Δ=${diff.toFixed(1)}K rH=${bleHum}%`);
            st.dryStart = now;
            st.lastCmdTs = now;
            await this._humCmd(folder, true, 'D', 'A');
            await setMode(MODI.DRY);
        }
        else if (st.mode === MODI.DRY && diff >= tpTief && bleHum < maxFeucht) {
            this.log.info(`Humidity [${folder}] stop DRY Δ=${diff.toFixed(1)}K rH=${bleHum}%`);
            await startNachlauf('ueberwachung');
            await setMode(MODI.NL);
        }
    }
    _taupunkt(T, rH) {
        const sdd = 6.112 * Math.exp((17.62 * T) / (243.12 + T));
        const dd = (sdd * rH) / 100.0;
        return (235.0 * Math.log(dd / 6.11)) / (17.1 - Math.log(dd / 6.11));
    }
    _humZeitErlaubt(start, end) {
        const now = new Date();
        const hm = now.getHours() * 60 + now.getMinutes();
        const [sh, sm] = (start || '07:00').split(':').map(Number);
        const [eh, em] = (end || '22:00').split(':').map(Number);
        const s = sh * 60 + sm, e = eh * 60 + em;
        return s < e ? hm >= s && hm < e : hm >= s || hm < e;
    }
    async _humCmd(folder, power, mode, fan) {
        try {
            await this.setStateAsync(`${folder}.power`, { val: power, ack: false });
            if (mode !== undefined) {
                await this.setStateAsync(`${folder}.mode`, { val: mode, ack: false });
            }
            if (fan !== undefined) {
                await this.setStateAsync(`${folder}.fan`, { val: fan, ack: false });
            }
        }
        catch (err) {
            this.log.warn(`Humidity [${folder}] cmd failed: ${String(err)}`);
        }
    }
    async onUnload(callback) {
        try {
            for (const ctx of this.demandCtxMap.values()) {
                await this.setStateAsync(`${ctx.folder}.demand_ctrl.active`, { val: false, ack: true });
            }
            if (this.mqttClient) {
                await this.mqttClient.disconnect();
                this.mqttClient = null;
            }
            await this.setStateAsync('info.connection', { val: false, ack: true });
        }
        catch (e) {
            this.log.error(`Error during unload: ${String(e)}`);
        }
        callback();
    }
}
if (require.main !== module) {
    module.exports = (options) => new FaikoutAdapter(options ?? {});
}
else {
    (() => new FaikoutAdapter())();
}
