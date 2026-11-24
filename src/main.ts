// Faikout ioBroker adapter - main entry point
//
// Discovery is NOT automatic on startup. It must be triggered manually per device
// via the "Discover" button in the Admin UI (Devices tab).
// On startup, stored topic information from previous discoveries is restored.

import * as utils from '@iobroker/adapter-core';
import { FaikoutMqttClient } from '../lib/mqttClient';
import type { DeviceProfile } from '../lib/deviceDiscovery';
import { discoverDevice } from '../lib/deviceDiscovery';
import { StateManager } from '../lib/stateManager';
import type { TopicPaths, TopicConfig } from '../lib/topicBuilder';
import { buildTopicPaths, DEFAULT_TOPIC_CONFIG } from '../lib/topicBuilder';
import type { FieldDef } from '../lib/fieldDefinitions';
import { FIELDS, CONFIG_FIELDS_DEF } from '../lib/fieldDefinitions';

interface DeviceConfig {
    name: string;
    ip: string;
    enabled: boolean;
    useDemand?: boolean;
    group?: string;
}

interface DemandCtx {
    folder: string;
    outdoorId: string;
    roomId: string;
    cfg: Record<string, unknown>;
    group: string; // empty = single-split / no group
    guardActive: boolean;
    running: boolean;
    lastRunTs: number;
    lastLogged: number | null;
    lastSent: number | null;
    lastChangeTs: number;
    lastEventTs: number;
}

class FaikoutAdapter extends utils.Adapter {
    private mqttClient: FaikoutMqttClient | null = null;
    private stateManagers: Map<string, StateManager> = new Map();
    // Map from MQTT status topic → device folder name
    private topicToFolder: Map<string, string> = new Map();
    // Map from device folder name → resolved topic paths (for publishing)
    private folderToTopics: Map<string, TopicPaths> = new Map();

    // Demand control – per-device contexts
    private demandCtxMap: Map<string, DemandCtx> = new Map(); // folder → ctx
    private outdoorIdToFolders: Map<string, string[]> = new Map(); // outdoorId → [folder…]
    private roomIdToFolders: Map<string, string[]> = new Map(); // roomId   → [folder…]
    private scheduleFired: Map<number, string> = new Map(); // index → "YYYY-MM-DD HH:MM"

    // Humidity control
    private humidityStates: Map<
        string,
        {
            cfg: Record<string, unknown>;
            mode: number;
            dryStart: number;
            nachlaufStart: number;
            pauseEnd: number;
            afterNachlauf: string;
            lastCmdTs: number;
            running: boolean;
        }
    > = new Map();

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({ ...options, name: 'faikout-ac' });
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    private async onReady(): Promise<void> {
        await this.setStateAsync('info.connection', { val: false, ack: true });

        const devices: DeviceConfig[] = this._sanitizeDeviceNames(
            ((this.config as Record<string, unknown>).devices as DeviceConfig[]) ?? [],
        );
        const enabledDevices = devices.filter((d) => d.enabled !== false);

        if (enabledDevices.length === 0) {
            this.log.warn('No devices configured. Please add at least one device in the adapter settings.');
        }

        // For each enabled device: create StateManager and restore topics from stored states.
        // Discovery is NOT run automatically – use the Discover button in the Admin UI per device.
        for (const device of enabledDevices) {
            const folder = device.name.replace(/[^a-zA-Z0-9_-]/g, '_');
            const sm = new StateManager(this, folder);
            this.stateManagers.set(folder, sm);
            await this._restoreTopicFromStates(folder);
        }

        // Create MQTT client
        const mqttHost = ((this.config as Record<string, unknown>).mqttHost as string) ?? 'localhost';
        const mqttPort = ((this.config as Record<string, unknown>).mqttPort as number) ?? 1883;
        const mqttUser = (this.config as Record<string, unknown>).mqttUser as string | undefined;
        const mqttPassword = (this.config as Record<string, unknown>).mqttPassword as string | undefined;
        const instanceId = this.instance ?? 0;

        this.mqttClient = new FaikoutMqttClient(
            {
                host: mqttHost,
                port: mqttPort,
                username: mqttUser || undefined,
                password: mqttPassword || undefined,
                clientId: `iobroker-faikout-${instanceId}`,
                setTimeoutFn: (cb, ms) => this.setTimeout(cb, ms),
                clearTimeoutFn: (id) => this.clearTimeout(id as ioBroker.Timeout),
            },
            (topic: string, payload: string) => {
                void this._handleMqttMessage(topic, payload).catch((e) => this.log.error(`MQTT message error: ${e}`));
            },
            (connected: boolean) => {
                void this._handleConnectionChange(connected).catch((e) =>
                    this.log.error(`Connection change error: ${e}`),
                );
            },
        );

        this.mqttClient.connect();

        for (const topic of this.topicToFolder.keys()) {
            this.mqttClient.subscribe(topic);
            this.log.info(`Subscribed to: ${topic}`);
        }

        // Set up demand control (subscribes foreign states if needed, creates states, initial run)
        await this._setupDemandControl();

        // Set up cyclic external sensor publishing (env_set)
        this._setupEnvSensors();

        // Set up schedule
        this._setupSchedule();

        // Set up humidity controls
        await this._setupHumidityControls();

        // Subscribe to all writable state changes
        await this.subscribeStatesAsync('*');

        // Initial demand calculation for all demand-enabled devices (ensures demand_calc / chart
        // working point is current immediately after startup or config change + restart)
        for (const folder of this.demandCtxMap.keys()) {
            void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
        }
    }

    private _sanitizeDeviceNames(devices: DeviceConfig[]): DeviceConfig[] {
        const usedNames = new Set<string>();
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

    private async _restoreTopicFromStates(folder: string): Promise<void> {
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
            const topicConfig: TopicConfig = {
                topicstate: topicStateState?.val ? String(topicStateState.val) : DEFAULT_TOPIC_CONFIG.topicstate,
                topiccommand: topicCommandState?.val
                    ? String(topicCommandState.val)
                    : DEFAULT_TOPIC_CONFIG.topiccommand,
                topicsetting: topicSettingState?.val
                    ? String(topicSettingState.val)
                    : DEFAULT_TOPIC_CONFIG.topicsetting,
                prefixapp: prefixAppState?.val === true,
                prefixhost: prefixHostState?.val === true,
            };

            const topics = buildTopicPaths(deviceId, appName, topicConfig);
            this.topicToFolder.set(topics.statusTopic, folder);
            this.folderToTopics.set(folder, topics);
            this.log.info(`Restored topic for "${folder}": ${topics.statusTopic}`);
        } catch (err) {
            this.log.warn(`Could not restore topic for "${folder}": ${String(err)}`);
        }
    }

    private async _handleMqttMessage(topic: string, payload: string): Promise<void> {
        const folder = this.topicToFolder.get(topic);
        if (!folder) {
            this.log.debug(`Received message on unknown topic: ${topic}`);
            return;
        }
        const sm = this.stateManagers.get(folder);
        if (!sm) {
            return;
        }

        // Skip if no discovery has been run yet (no channel object = no states to update)
        const deviceObj = await this.getObjectAsync(folder);
        if (!deviceObj) {
            this.log.debug(`Skipping MQTT message for "${folder}" – run Discovery first`);
            return;
        }

        // LWT: device publishes {"up":false,"reason":"LWT"} on the status topic when disconnected
        try {
            const json = JSON.parse(payload) as Record<string, unknown>;
            if (json.up === false) {
                await this.setStateAsync(`${folder}.info.connection`, { val: false, ack: true });
                this.log.info(`Device "${folder}" disconnected (LWT)`);
                return;
            }
        } catch {
            // not JSON or no up field – continue normally
        }

        await sm.updateFromMqttPayload(payload);
        await this.setStateAsync(`${folder}.info.connection`, { val: true, ack: true });
    }

    private async _handleConnectionChange(connected: boolean): Promise<void> {
        await this.setStateAsync('info.connection', { val: connected, ack: true });
        if (connected) {
            this.log.info('MQTT connected');
            // Demand control: activate lockmode on (re)connect for all demand devices
            for (const ctx of this.demandCtxMap.values()) {
                this._publishDemandConfigParam(ctx.folder, 'lockmode', '1');
                if (ctx.cfg.demandLogLevel !== 'off') {
                    this.log.info(`Demand control: lockmode activated for "${ctx.folder}"`);
                }
            }
        } else {
            this.log.warn('MQTT disconnected');
            for (const folder of this.stateManagers.keys()) {
                try {
                    await this.setStateAsync(`${folder}.info.connection`, { val: false, ack: true });
                } catch {
                    // Ignore errors during shutdown
                }
            }
        }
    }

    // Handle state writes: convert value and publish to the correct MQTT topic
    private onStateChange(id: string, state: ioBroker.State | null | undefined): void {
        // Demand control: react to sensor temp changes
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

        // Demand control: react to power/mode changes on any demand device
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

        // Demand control: handle override state writes (ack these ourselves, then recalculate)
        if (state && !state.ack && this.demandCtxMap.size > 0) {
            for (const ctx of this.demandCtxMap.values()) {
                const dp = `${this.namespace}.${ctx.folder}.demand_ctrl.`;
                if (id.startsWith(dp)) {
                    void (async () => {
                        await this.setStateAsync(id.slice(`${this.namespace}.`.length), { val: state.val, ack: true });
                        void this._runDemandControl(ctx.folder).catch((e) =>
                            this.log.error(`Demand control error: ${e}`),
                        );
                    })().catch((e) => this.log.error(`Demand override error: ${e}`));
                    return;
                }
            }
        }

        // Humidity control: handle auto state write
        if (state && !state.ack) {
            for (const [folder] of this.humidityStates) {
                if (id === `${this.namespace}.${folder}.humidity_ctrl.auto`) {
                    void (async () => {
                        await this.setStateAsync(`${folder}.humidity_ctrl.auto`, { val: state.val, ack: true });
                        void this._runHumidityControl(folder).catch((e) =>
                            this.log.error(`Humidity control error: ${e}`),
                        );
                    })().catch((e) => this.log.error(`Humidity auto error: ${e}`));
                    return;
                }
            }
        }
        // Humidity control: trigger on in-namespace sensor updates
        if (state?.ack && this.humidityStates.size > 0) {
            for (const [folder, st] of this.humidityStates) {
                const ns = `${this.namespace}.`;
                const tempId = ((st.cfg.humTempStateId as string) || '').trim() || `${ns}${folder}.ble.temp`;
                const humId = ((st.cfg.humHumStateId as string) || '').trim() || `${ns}${folder}.ble.hum`;
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

    private _handleStateWrite(id: string, val: ioBroker.StateValue): void {
        if (!this.mqttClient) {
            return;
        }

        // State ID format: faikout-ac.0.{folder}.{field}
        //              or: faikout-ac.0.{folder}.config.{configField}
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

        // Config field: {folder}.config.{configField}
        if (parts[1] === 'config' && parts.length === 3) {
            const configField = parts[2];
            if (!CONFIG_FIELDS_DEF[configField]) {
                return;
            }
            const topic = topics.settingTopic(configField);
            const payload = String(val ?? '');
            this.mqttClient.publish(topic, payload);
            this.log.debug(`Config write: ${topic} = ${payload}`);
            return;
        }

        // Regular writable field: {folder}.{field}
        if (parts.length !== 2) {
            return;
        }
        const fieldName = parts[1];
        const def = FIELDS[fieldName];
        if (!def?.writeTopic) {
            return;
        }

        const payload = this._buildWritePayload(fieldName, def, val);
        if (payload === null) {
            return;
        }

        let topic: string;
        if (def.writeTopic.startsWith('setting:')) {
            // e.g. 'setting:autot' → settingTopic('autot')
            const settingField = def.writeTopic.split(':')[1];
            topic = topics.settingTopic(settingField);
        } else if (def.writeTopic === 'setting') {
            // auto0, auto1 → base setting topic with JSON payload
            topic = topics.settingJsonTopic;
        } else {
            // control, mode, fan, econo, powerful, etc. → commandTopic
            topic = topics.commandTopic(def.writeTopic);
        }

        this.mqttClient.publish(topic, payload);
        this.log.debug(`State write: ${topic} = ${payload}`);
    }

    // Convert a state value to the correct MQTT payload string.
    // Mirrors the writeConv expressions from faikout_alias_generator_en.js.
    private _buildWritePayload(key: string, def: FieldDef, val: ioBroker.StateValue): string | null {
        if (val === null || val === undefined) {
            return null;
        }

        if (def.writeTopic === 'control') {
            // power, swingv, swingh → JSON.stringify({fieldName: val})
            // env_set              → JSON.stringify({env: val})
            const jsonKey = key === 'env_set' ? 'env' : key;
            return JSON.stringify({ [jsonKey]: val });
        }

        if (def.writeTopic === 'setting') {
            // auto0, auto1: strip colon from time strings (e.g. "08:30" → "0830")
            return JSON.stringify({ [key]: String(val ?? '').replace(':', '') });
        }

        if (def.writeTopic?.startsWith('setting:')) {
            // autot, autor: direct numeric value
            return String(val);
        }

        // All other command topics (econo, powerful, comfort, streamer, led, quiet,
        // autoe, autop, restart): boolean → 1/0, others → direct string
        if (def.type === 'boolean') {
            return val ? '1' : '0';
        }
        return String(val);
    }

    // Handle sendTo messages from Admin UI
    private onMessage(obj: ioBroker.Message): void {
        if (obj.command === 'discovery') {
            const msg = obj.message as Record<string, string> | undefined;
            const name = msg?.name;

            if (!name) {
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, { error: 'No device name provided' }, obj.callback);
                }
                return;
            }

            const devices: DeviceConfig[] = ((this.config as Record<string, unknown>).devices as DeviceConfig[]) ?? [];
            const device = devices.find((d) => d.name === name);
            if (!device?.ip) {
                if (obj.callback) {
                    this.sendTo(
                        obj.from,
                        obj.command,
                        { error: `Device "${name}" not found in configuration` },
                        obj.callback,
                    );
                }
                return;
            }

            const ip = device.ip;
            this.log.info(`Discovery triggered for "${name}" at ${ip}`);

            discoverDevice(ip)
                .then(async (profile: DeviceProfile) => {
                    const folder = name.replace(/[^a-zA-Z0-9_-]/g, '_');

                    let sm = this.stateManagers.get(folder);
                    if (!sm) {
                        sm = new StateManager(this, folder);
                        this.stateManagers.set(folder, sm);
                    }

                    await sm.createStates(profile, true);

                    await this.setStateAsync(`${folder}.info.deviceId`, { val: profile.deviceId, ack: true });
                    await this.setStateAsync(`${folder}.info.appName`, { val: profile.appName, ack: true });
                    await this.setStateAsync(`${folder}.info.ip`, { val: ip, ack: true });

                    const topics = buildTopicPaths(profile.deviceId, profile.appName, profile.topicConfig);
                    if (!this.topicToFolder.has(topics.statusTopic)) {
                        this.topicToFolder.set(topics.statusTopic, folder);
                        if (this.mqttClient) {
                            this.mqttClient.subscribe(topics.statusTopic);
                            this.log.info(`Subscribed to: ${topics.statusTopic}`);
                        }
                    }
                    this.folderToTopics.set(folder, topics);

                    // Activate live status push on device (write through normal config state pipeline)
                    await this.setStateAsync(`${folder}.config.livestatus`, { val: true, ack: false });

                    this.log.info(
                        `Discovery completed for "${name}": ${profile.deviceId} (${profile.appName}) – ${profile.activeFields.size} fields`,
                    );

                    if (obj.callback) {
                        this.sendTo(
                            obj.from,
                            obj.command,
                            {
                                result: `Discovered: ${profile.deviceId} (${profile.appName || 'unknown'}) – ${profile.activeFields.size} fields`,
                            },
                            obj.callback,
                        );
                    }
                })
                .catch((err: Error) => {
                    this.log.error(`Discovery failed for "${name}" (${ip}): ${err.message}`);
                    if (obj.callback) {
                        this.sendTo(obj.from, obj.command, { error: err.message }, obj.callback);
                    }
                });
        }

        if (obj.command === 'getDeviceList') {
            const devices: DeviceConfig[] = ((this.config as Record<string, unknown>).devices as DeviceConfig[]) ?? [];
            if (obj.callback) {
                this.sendTo(
                    obj.from,
                    obj.command,
                    devices.map((d) => ({ label: d.name, value: d.name })),
                    obj.callback,
                );
            }
        }

        if (obj.command === 'deleteDevice') {
            const msg = obj.message as Record<string, string> | undefined;
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
                    // Check for custom data (History/InfluxDB/SQL) before deleting
                    const allObjs = await this.getObjectListAsync({
                        startkey: `${this.namespace}.${folder}.`,
                        endkey: `${this.namespace}.${folder}.\u9999`,
                    });
                    const withCustom = (allObjs?.rows ?? []).filter((r) => {
                        const custom = (r.value?.common as ioBroker.StateCommon | undefined)?.custom;
                        return custom && Object.keys(custom).length > 0;
                    });
                    if (withCustom.length > 0) {
                        const ids = withCustom.map((r) => r.id).join(', ');
                        this.log.warn(
                            `Device "${name}": ${withCustom.length} state(s) have custom data (History/InfluxDB) – deletion skipped to preserve it. Affected: ${ids}`,
                        );
                        if (obj.callback) {
                            this.sendTo(
                                obj.from,
                                obj.command,
                                {
                                    error: `Cannot delete: ${withCustom.length} state(s) have History/InfluxDB custom data. Remove the custom data first.`,
                                },
                                obj.callback,
                            );
                        }
                        return;
                    }

                    await this.delObjectAsync(folder, { recursive: true });

                    // Clean up internal maps
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
                } catch (err) {
                    this.log.error(`Failed to delete states for "${name}": ${String(err)}`);
                    if (obj.callback) {
                        this.sendTo(obj.from, obj.command, { error: String(err) }, obj.callback);
                    }
                }
            })();
        }
    }

    private _setupSchedule(): void {
        const cfg = this.config as Record<string, unknown>;
        const entries = (cfg.schedule as Array<Record<string, unknown>> | undefined) || [];
        if (entries.length === 0) {
            return;
        }

        const sceneModes = (cfg.sceneModes as Array<Record<string, unknown>> | undefined) || [];

        this.setInterval(async () => {
            const now = new Date();
            // 0=Sun,1=Mon…6=Sat → remap to 0=Mon…6=Sun to match UI (Mo=0…So=6)
            const dow = (now.getDay() + 6) % 7;
            const hhmm = now.toTimeString().slice(0, 5); // "HH:MM"
            const dateKey = now.toISOString().slice(0, 10); // "YYYY-MM-DD"
            const firedKey = `${dateKey} ${hhmm}`;

            for (let i = 0; i < entries.length; i++) {
                const entry = entries[i];
                if (!entry.enabled) {
                    continue;
                }
                const entryScene = ((entry.scene as string) || '').trim();
                if (entryScene) {
                    const mode = sceneModes.find((m) => (m.name as string) === entryScene);
                    if (!mode || !mode.active) {
                        continue;
                    }
                }
                const entryTime = ((entry.time as string) || '').trim();
                const entryDays = (entry.days as number[] | undefined) || [];
                if (entryTime !== hhmm) {
                    continue;
                }
                if (!entryDays.includes(dow)) {
                    continue;
                }
                if (this.scheduleFired.get(i) === firedKey) {
                    continue;
                } // already fired this minute

                const deviceName = ((entry.deviceName as string) || '').trim();
                // support both new multi-action format and legacy single field/value
                const actions: Array<Record<string, unknown>> = Array.isArray(entry.actions)
                    ? (entry.actions as Array<Record<string, unknown>>)
                    : entry.field
                      ? [{ field: entry.field, value: entry.value }]
                      : [];
                if (!deviceName || actions.length === 0) {
                    continue;
                }

                const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
                let fired = false;
                for (const action of actions) {
                    const field = ((action.field as string) || '').trim();
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
                        } else if (field === 'demand_override_off') {
                            await this.setStateAsync(`${folder}.demand_ctrl.override_active`, {
                                val: false,
                                ack: false,
                            });
                            this.log.info(`Schedule [${deviceName}] demand override OFF`);
                        } else {
                            await this.setStateAsync(`${folder}.${field}`, {
                                val: value as ioBroker.StateValue,
                                ack: false,
                            });
                            this.log.info(`Schedule [${deviceName}] ${field}=${JSON.stringify(value)}`);
                        }
                        fired = true;
                    } catch (err) {
                        this.log.warn(`Schedule [${deviceName}] ${field} failed: ${String(err)}`);
                    }
                    const delayMs = Math.max(0, Number(action.delay ?? 0)) * 1000;
                    if (delayMs > 0) {
                        await new Promise<void>((resolve) => this.setTimeout(resolve, delayMs));
                    }
                }
                if (fired) {
                    this.scheduleFired.set(i, firedKey);
                }
            }
        }, 30_000);
    }

    private _setupEnvSensors(): void {
        const cfg = this.config as Record<string, unknown>;
        const entries = (cfg.envSensors as Array<Record<string, unknown>> | undefined) || [];
        for (const entry of entries) {
            const deviceName = ((entry.deviceName as string) || '').trim();
            const sensorStateId = ((entry.sensorStateId as string) || '').trim();
            const intervalSec = Math.min(Math.max(10, Number(entry.intervalSec ?? 30)), 2_147_483);
            if (!deviceName || !sensorStateId) {
                continue;
            }
            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const targetId = `${this.namespace}.${folder}.env_set`;
            const publish = async (): Promise<void> => {
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
                } catch {
                    /* ignore */
                }
            };
            void publish();
            this.setInterval(publish, intervalSec * 1_000);
            this.log.info(`EnvSensor [${deviceName}] → ${sensorStateId} every ${intervalSec}s`);
        }
    }

    private async _setupDemandControl(): Promise<void> {
        const cfg = this.config as Record<string, unknown>;
        const devices = (cfg.devices as Array<Record<string, unknown>>) || [];
        const demandDevices = devices.filter((d) => d.enabled !== false && d.useDemand);

        if (demandDevices.length === 0) {
            this.log.info('Demand control: no device has useDemand checkbox set – skipping');
            return;
        }

        // Flat legacy demand fields (demand*) serve as defaults – sensor IDs excluded (device-specific)
        const FLAT_SKIP = new Set(['demandOutdoorTempId', 'demandRoomTempId']);
        const flatDefaults: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(cfg)) {
            if (k.startsWith('demand') && !FLAT_SKIP.has(k)) {
                flatDefaults[k] = v;
            }
        }

        for (const deviceEntry of demandDevices) {
            const deviceName = ((deviceEntry.name as string) || '').trim();
            if (!deviceName) {
                this.log.warn('Demand control: device name empty – skipping entry');
                continue;
            }

            // Merge: flat defaults → per-device cfg[deviceName]
            const devCfg: Record<string, unknown> = Object.assign(
                {},
                flatDefaults,
                (cfg[deviceName] as Record<string, unknown> | undefined) || {},
            );

            if (!devCfg.demandEnabled) {
                this.log.info(`Demand control: demandEnabled not set for "${deviceName}" – skipping`);
                continue;
            }

            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const group = ((deviceEntry.group as string) || '').trim();
            const outdoorId =
                ((devCfg.demandOutdoorTempId as string) || '').trim() || `${this.namespace}.${folder}.outside`;
            const roomId = ((devCfg.demandRoomTempId as string) || '').trim() || `${this.namespace}.${folder}.env`;

            const ctx: DemandCtx = {
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

            // Sensor ID → folder maps (for onStateChange routing)
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

            // Create demand_ctrl states
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
            const stBase = { type: 'state' as const, native: {} };
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

            // Fallback timer: fire every 180s if no sensor event has arrived
            this.setInterval(() => {
                if (Date.now() - ctx.lastEventTs >= 180_000) {
                    void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
                }
            }, 180_000);

            this.log.info(`Demand control active for "${deviceName}" | outdoor: ${outdoorId} | room: ${roomId}`);
            void this._runDemandControl(folder).catch((e) => this.log.error(`Demand control error: ${e}`));
        }
    }

    private async _runDemandControl(folder: string): Promise<void> {
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
        } finally {
            ctx.running = false;
        }
    }

    private async _runDemandControlCore(ctx: DemandCtx): Promise<void> {
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

        // Read power/mode early – needed to decide whether to send, but calculation runs regardless
        const powerState = await this.getStateAsync(`${folder}.power`);
        const modeState = await this.getStateAsync(`${folder}.mode`);
        const isActive = powerState?.val === true && modeState?.val === 'H';

        // 1. Base demand from curve interpolation (always calculated for chart display)
        const curve = (cfg.demandCurve as Array<{ temp: number; demand: number }> | undefined) ?? [];
        if (curve.length < 2) {
            this.log.warn('Demand curve has fewer than 2 support points – skipping');
            return;
        }
        let demand = this._interpolateCurve(curve, outdoor);
        const baseDemand = demand;

        // 2. Boost
        let boostApplied = 0;
        let boostStageDesc = '';
        if (cfg.demandBoostEnabled) {
            const autotState = await this.getStateAsync(`${folder}.autot`);
            if (autotState?.val != null) {
                const autot = Number(autotState.val);
                const diff = autot - room;
                const stages = (cfg.demandBoostStages as Array<{ deviation: number; boost: number }> | undefined) ?? [];
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

        // 3. Taper
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

        // 4. Guard
        await this._checkGuard(ctx, room);

        // Clamp to [min, max] demand
        demand = Math.min(Number(cfg.demandMaxDemand ?? 95), demand);
        demand = Math.max(Number(cfg.demandMinDemand ?? 30), demand);
        demand = Math.round(demand);

        // 5. Multi-split group floor: demand >= max(minDemand of all enabled peers in same group)
        if (ctx.group) {
            let groupFloor = demand;
            for (const peer of this.demandCtxMap.values()) {
                if (peer.folder !== folder && peer.group === ctx.group) {
                    groupFloor = Math.max(groupFloor, Number(peer.cfg.demandMinDemand ?? 30));
                }
            }
            demand = Math.max(demand, groupFloor);
        }

        // Always update display states (chart working point) – no log, no device write
        await this.setStateAsync(`${dp}.guard_active`, { val: ctx.guardActive, ack: true });
        await this.setStateAsync(`${dp}.demand_calc`, { val: demand, ack: true });

        // Only send to device when active in heating mode
        if (!isActive) {
            this.log.debug(
                `Demand control skipped: power=${String(powerState?.val)} mode=${String(modeState?.val ?? '?')}`,
            );
            return;
        }

        // Manual override: skip curve result, use override value directly
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
            this.log.info(
                `Demand [${folder}] outdoor=${outdoor}°C room=${room}°C` +
                    ` | base=${baseDemand.toFixed(0)}% boost=${boostStr} taper=${taperStr}` +
                    ` → target=${demand}% | guard=${guardStr}`,
            );
        } else if (demand !== ctx.lastLogged) {
            this.log.info(`Demand [${folder}] outdoor=${outdoor}°C room=${room}°C → target=${demand}%`);
            ctx.lastLogged = demand;
        }

        this._scheduleDemand(ctx, demand);
    }

    // Rate-controlled demand scheduling.
    // Each change (up or down) is held for its configured delay since the last change.
    private _scheduleDemand(ctx: DemandCtx, target: number): void {
        const { cfg, folder } = ctx;
        const delayDown = Number(cfg.demandDelayReduce ?? 0) * 1000;
        const delayUp = Number(cfg.demandDelayIncrease ?? 300) * 1000;

        // First send: apply immediately
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
        } else {
            this.log.info(
                `Demand [${folder}] ${goingUp ? '↑' : '↓'} ${ctx.lastSent}%→${target}% gehalten, noch ${Math.round((delay - elapsed) / 1000)}s`,
            );
        }
    }

    private _sendDemand(ctx: DemandCtx, demand: number): void {
        ctx.lastSent = demand;
        ctx.lastChangeTs = Date.now();
        void this.setStateAsync(`${ctx.folder}.demand`, { val: demand, ack: false }).catch(() => {
            this.log.warn(`Demand state not found for "${ctx.folder}" – run Discovery first`);
        });
        this.log.debug(`Demand [${ctx.folder}] sent=${demand}%`);
    }

    private async _checkGuard(ctx: DemandCtx, room: number): Promise<void> {
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
                this.log.info(
                    `Demand guard ON: room=${room}°C >= threshold=${guardThreshold.toFixed(2)}°C → heatback=${heatbackGuard}`,
                );
            }
        } else if (room <= autot && ctx.guardActive) {
            ctx.guardActive = false;
            this._publishGuardHeatback(folder, heatbackNormal);
            if (!logOff) {
                this.log.info(`Demand guard OFF: room=${room}°C <= autot=${autot}°C → heatback=${heatbackNormal}`);
            }
        }
    }

    private _interpolateCurve(curve: Array<{ temp: number; demand: number }>, x: number): number {
        const sorted = [...curve].sort((a, b) => Number(a.temp) - Number(b.temp));
        if (x <= Number(sorted[0].temp)) {
            return Number(sorted[0].demand);
        }
        if (x >= Number(sorted[sorted.length - 1].temp)) {
            return Number(sorted[sorted.length - 1].demand);
        }
        for (let i = 0; i < sorted.length - 1; i++) {
            const t0 = Number(sorted[i].temp),
                t1 = Number(sorted[i + 1].temp);
            if (x >= t0 && x <= t1) {
                const frac = (x - t0) / (t1 - t0);
                return Number(sorted[i].demand) + frac * (Number(sorted[i + 1].demand) - Number(sorted[i].demand));
            }
        }
        return 0;
    }

    private _publishGuardHeatback(folder: string, value: number): void {
        this._publishDemandConfigParam(folder, 'heatback', String(value));
        this.log.debug(`Guard heatback → ${value}`);
    }

    private _publishDemandConfigParam(folder: string, param: string, value: string): void {
        const topics = this.folderToTopics.get(folder);
        if (!topics || !this.mqttClient) {
            return;
        }
        this.mqttClient.publish(topics.settingTopic(param), value);
    }

    private async _setupHumidityControls(): Promise<void> {
        const cfg = this.config as Record<string, unknown>;
        const devices = (cfg.devices as Array<Record<string, unknown>>) || [];
        const humCfg = (cfg.humCfg as Record<string, Record<string, unknown>> | undefined) ?? {};

        const humDevices = devices.filter((d) => d.enabled !== false && d.useHumidity);
        if (humDevices.length === 0) {
            return;
        }

        for (const device of humDevices) {
            const deviceName = ((device.name as string) || '').trim();
            if (!deviceName) {
                continue;
            }
            const folder = deviceName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const dp = `${folder}.humidity_ctrl`;

            const devCfg: Record<string, unknown> = Object.assign(
                {
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
                },
                humCfg[deviceName] ?? {},
            );

            const stBase = { type: 'state' as const, native: {} };
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
                    type: 'boolean' as const,
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
                    type: 'number' as const,
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
                    type: 'string' as const,
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
                    type: 'number' as const,
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
                    type: 'number' as const,
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

    private async _runHumidityControl(folder: string): Promise<void> {
        const st = this.humidityStates.get(folder);
        if (!st || st.running) {
            return;
        }
        st.running = true;
        try {
            await this._runHumidityControlCore(folder, st);
        } finally {
            st.running = false;
        }
    }

    private async _runHumidityControlCore(
        folder: string,
        st: {
            cfg: Record<string, unknown>;
            mode: number;
            dryStart: number;
            nachlaufStart: number;
            pauseEnd: number;
            afterNachlauf: string;
            lastCmdTs: number;
        },
    ): Promise<void> {
        const MODI = { AUS: 0, UE: 1, DRY: 2, NL: 3, ZP: 4, FF: 5 };
        const MOD_NAME = ['OFF', 'MONITORING', 'DRY', 'RUN-ON', 'PAUSE', 'FRESHNESS!'];
        const cfg = st.cfg;
        const dp = `${folder}.humidity_ctrl`;
        const ns = `${this.namespace}.`;
        const toMs = (m: number): number => m * 60_000;
        const now = Date.now();

        const setMode = async (m: number): Promise<void> => {
            st.mode = m;
            await this.setStateAsync(`${dp}.mode`, { val: m, ack: true });
            await this.setStateAsync(`${dp}.mode_text`, { val: MOD_NAME[m] ?? '?', ack: true });
        };

        const autoState = await this.getStateAsync(`${dp}.auto`);
        const auto = autoState?.val === true;

        const tempId = ((cfg.humTempStateId as string) || '').trim() || `${ns}${folder}.ble.temp`;
        const humId = ((cfg.humHumStateId as string) || '').trim() || `${ns}${folder}.ble.hum`;
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

        const startNachlauf = async (reason: string): Promise<void> => {
            st.nachlaufStart = now;
            st.dryStart = 0;
            st.afterNachlauf = reason;
            await this._humCmd(folder, true, 'F', 'A');
        };

        // 1. Nachlauf timer
        if (st.nachlaufStart > 0 && now - st.nachlaufStart >= toMs(Number(cfg.humNachlaufMin ?? 15))) {
            await this._humCmd(folder, false);
            st.nachlaufStart = 0;
            const reason = st.afterNachlauf;
            st.afterNachlauf = 'ueberwachung';
            if (st.mode === MODI.FF) {
                await this.setStateAsync(`${dp}.auto`, { val: false, ack: true });
                await setMode(MODI.AUS);
            } else if (reason === 'zwangspause') {
                st.pauseEnd = now + toMs(Number(cfg.humPauseMin ?? 30));
                await setMode(MODI.ZP);
            } else {
                await setMode(auto ? MODI.UE : MODI.AUS);
            }
            return;
        }
        // 2. Zwangspause timer
        if (st.mode === MODI.ZP && st.pauseEnd > 0 && now >= st.pauseEnd) {
            st.pauseEnd = 0;
            await setMode(MODI.UE);
        }
        // 3. Auto off
        if (!auto) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('ueberwachung');
                await setMode(MODI.NL);
            } else if (![MODI.NL, MODI.ZP, MODI.FF].includes(st.mode)) {
                if (st.mode !== MODI.AUS) {
                    await this._humCmd(folder, false);
                    await setMode(MODI.AUS);
                }
            }
            return;
        }
        // 4. Freshness
        if (stale && st.mode !== MODI.FF) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('freshness');
                await setMode(MODI.NL);
            }
            await setMode(MODI.FF);
            this.log.warn(`Humidity [${folder}] sensor stale`);
            return;
        }
        // 5. Skip special states
        if ([MODI.FF, MODI.NL, MODI.ZP].includes(st.mode)) {
            return;
        }
        // 6. No data
        if (bleTemp === undefined || bleHum === undefined) {
            if (st.mode === MODI.AUS) {
                await this.setStateAsync(`${dp}.auto`, { val: false, ack: true });
            }
            return;
        }
        // AUS → ÜBERWACHUNG
        if (st.mode === MODI.AUS) {
            await setMode(MODI.UE);
        }
        // 7. Dew point
        const tp = this._taupunkt(bleTemp, bleHum);
        const diff = bleTemp - tp;
        await this.setStateAsync(`${dp}.dew_point`, { val: Math.round(tp * 10) / 10, ack: true });
        await this.setStateAsync(`${dp}.dew_point_diff`, { val: Math.round(diff * 10) / 10, ack: true });
        this.log.debug(
            `Humidity [${folder}] T=${bleTemp}°C rH=${bleHum}% TP=${tp.toFixed(1)}° Δ=${diff.toFixed(1)}K mode=${MOD_NAME[st.mode]}`,
        );
        // 8. Time window
        if (!this._humZeitErlaubt((cfg.humStartZeit as string) || '07:00', (cfg.humEndZeit as string) || '22:00')) {
            if (st.mode === MODI.DRY) {
                await startNachlauf('ueberwachung');
                await setMode(MODI.NL);
            }
            return;
        }
        // 9. Max runtime
        if (st.mode === MODI.DRY && st.dryStart > 0 && now - st.dryStart >= toMs(Number(cfg.humMaxLaufzeit ?? 240))) {
            this.log.info(`Humidity [${folder}] max runtime – forced pause`);
            await startNachlauf('zwangspause');
            await setMode(MODI.NL);
            return;
        }
        // 10. Override detection
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
        // 11. Control logic
        const tpHoch = Number(cfg.humTpHoch ?? 4.0);
        const tpTief = Number(cfg.humTpTief ?? 7.0);
        const maxFeucht = Number(cfg.humMaxFeuchte ?? 75);
        if (st.mode === MODI.UE && (diff <= tpHoch || bleHum >= maxFeucht)) {
            this.log.info(`Humidity [${folder}] start DRY Δ=${diff.toFixed(1)}K rH=${bleHum}%`);
            st.dryStart = now;
            st.lastCmdTs = now;
            await this._humCmd(folder, true, 'D', 'A');
            await setMode(MODI.DRY);
        } else if (st.mode === MODI.DRY && diff >= tpTief && bleHum < maxFeucht) {
            this.log.info(`Humidity [${folder}] stop DRY Δ=${diff.toFixed(1)}K rH=${bleHum}%`);
            await startNachlauf('ueberwachung');
            await setMode(MODI.NL);
        }
    }

    private _taupunkt(T: number, rH: number): number {
        const sdd = 6.112 * Math.exp((17.62 * T) / (243.12 + T));
        const dd = (sdd * rH) / 100.0;
        return (235.0 * Math.log(dd / 6.11)) / (17.1 - Math.log(dd / 6.11));
    }

    private _humZeitErlaubt(start: string, end: string): boolean {
        const now = new Date();
        const hm = now.getHours() * 60 + now.getMinutes();
        const [sh, sm] = (start || '07:00').split(':').map(Number);
        const [eh, em] = (end || '22:00').split(':').map(Number);
        const s = sh * 60 + sm,
            e = eh * 60 + em;
        return s < e ? hm >= s && hm < e : hm >= s || hm < e;
    }

    private async _humCmd(folder: string, power: boolean, mode?: string, fan?: string): Promise<void> {
        try {
            await this.setStateAsync(`${folder}.power`, { val: power, ack: false });
            if (mode !== undefined) {
                await this.setStateAsync(`${folder}.mode`, { val: mode, ack: false });
            }
            if (fan !== undefined) {
                await this.setStateAsync(`${folder}.fan`, { val: fan, ack: false });
            }
        } catch (err) {
            this.log.warn(`Humidity [${folder}] cmd failed: ${String(err)}`);
        }
    }

    private async onUnload(callback: () => void): Promise<void> {
        try {
            for (const ctx of this.demandCtxMap.values()) {
                await this.setStateAsync(`${ctx.folder}.demand_ctrl.active`, { val: false, ack: true });
            }
            if (this.mqttClient) {
                await this.mqttClient.disconnect();
                this.mqttClient = null;
            }
            await this.setStateAsync('info.connection', { val: false, ack: true });
        } catch (e) {
            this.log.error(`Error during unload: ${String(e)}`);
        }
        callback();
    }
}

if (require.main !== module) {
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new FaikoutAdapter(options ?? {});
} else {
    (() => new FaikoutAdapter())();
}
