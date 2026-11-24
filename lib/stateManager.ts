// State manager for Faikout ioBroker adapter
// Manages ioBroker states for a single device.
// Creates, updates and cleans up states based on DeviceProfile.

import type { FieldDef } from './fieldDefinitions';
import { FIELDS, CONFIG_FIELDS_DEF, getFanStates } from './fieldDefinitions';
import type { DeviceProfile } from './deviceDiscovery';

type Adapter = ioBroker.Adapter;

export class StateManager {
    private readonly adapter: Adapter;
    private readonly deviceFolder: string;

    constructor(adapter: Adapter, deviceFolder: string) {
        this.adapter = adapter;
        this.deviceFolder = deviceFolder;
    }

    // Create or update all states for a discovered device profile
    async createStates(profile: DeviceProfile, createConfigStates: boolean): Promise<void> {
        const { deviceId, appName, ip, activeFields, topicConfig, fantype } = profile;

        // Device channel
        await this.adapter.setObjectNotExistsAsync(this.deviceFolder, {
            type: 'channel',
            common: { name: appName || deviceId },
            native: {},
        });

        // Info channel
        await this._ensureChannel(`${this.deviceFolder}.info`, {
            en: 'Device Information',
            de: 'Geräteinformationen',
            ru: 'Информация об устройстве',
            pt: 'Informações do dispositivo',
            nl: 'Apparaatinformatie',
            fr: "Informations sur l'appareil",
            it: 'Informazioni dispositivo',
            es: 'Información del dispositivo',
            pl: 'Informacje o urządzeniu',
            uk: 'Інформація про пристрій',
            'zh-cn': '设备信息',
        });
        await this._ensureState(`${this.deviceFolder}.info.deviceId`, {
            name: {
                en: 'Device ID',
                de: 'Geräte-ID',
                ru: 'ID устройства',
                pt: 'ID do dispositivo',
                nl: 'Apparaat-ID',
                fr: "ID de l'appareil",
                it: 'ID dispositivo',
                es: 'ID del dispositivo',
                pl: 'ID urządzenia',
                uk: 'ID пристрою',
                'zh-cn': '设备 ID',
            },
            type: 'string',
            role: 'info.address',
            read: true,
            write: false,
            def: deviceId,
        });
        await this._ensureState(`${this.deviceFolder}.info.appName`, {
            name: {
                en: 'App Name',
                de: 'App-Name',
                ru: 'Имя приложения',
                pt: 'Nome do aplicativo',
                nl: 'App-naam',
                fr: "Nom de l'application",
                it: 'Nome app',
                es: 'Nombre de la app',
                pl: 'Nazwa aplikacji',
                uk: 'Назва програми',
                'zh-cn': '应用名称',
            },
            type: 'string',
            role: 'info.name',
            read: true,
            write: false,
            def: appName,
        });
        await this._ensureState(`${this.deviceFolder}.info.ip`, {
            name: {
                en: 'IP Address',
                de: 'IP-Adresse',
                ru: 'IP-адрес',
                pt: 'Endereço IP',
                nl: 'IP-adres',
                fr: 'Adresse IP',
                it: 'Indirizzo IP',
                es: 'Dirección IP',
                pl: 'Adres IP',
                uk: 'IP-адреса',
                'zh-cn': 'IP 地址',
            },
            type: 'string',
            role: 'info.ip',
            read: true,
            write: false,
            def: ip,
        });
        await this._ensureState(`${this.deviceFolder}.info.connection`, {
            name: {
                en: 'Connection',
                de: 'Verbindung',
                ru: 'Соединение',
                pt: 'Conexão',
                nl: 'Verbinding',
                fr: 'Connexion',
                it: 'Connessione',
                es: 'Conexión',
                pl: 'Połączenie',
                uk: 'Зʼєднання',
                'zh-cn': '连接',
            },
            type: 'boolean',
            role: 'indicator.reachable',
            read: true,
            write: false,
            def: false,
        });
        await this._ensureState(`${this.deviceFolder}.info.lastUpdate`, {
            name: {
                en: 'Last Update',
                de: 'Letztes Update',
                ru: 'Последнее обновление',
                pt: 'Última atualização',
                nl: 'Laatste update',
                fr: 'Dernière mise à jour',
                it: 'Ultimo aggiornamento',
                es: 'Última actualización',
                pl: 'Ostatnia aktualizacja',
                uk: 'Останнє оновлення',
                'zh-cn': '最后更新',
            },
            type: 'number',
            role: 'date',
            read: true,
            write: false,
            def: 0,
        });
        await this._ensureState(`${this.deviceFolder}.info.fantype`, {
            name: {
                en: 'Fan type',
                de: 'Lüftertyp',
                ru: 'Тип вентилятора',
                pt: 'Tipo de ventilador',
                nl: 'Ventilatortype',
                fr: 'Type de ventilateur',
                it: 'Tipo ventilatore',
                es: 'Tipo de ventilador',
                pl: 'Typ wentylatora',
                uk: 'Тип вентилятора',
                'zh-cn': '风扇类型',
            },
            type: 'number',
            role: 'value',
            read: true,
            write: false,
            def: fantype,
        });
        await this.adapter.setStateAsync(`${this.deviceFolder}.info.fantype`, { val: fantype, ack: true });
        await this._ensureState(`${this.deviceFolder}.info.protocol`, {
            name: {
                en: 'Protocol',
                de: 'Protokoll',
                ru: 'Протокол',
                pt: 'Protocolo',
                nl: 'Protocol',
                fr: 'Protocole',
                it: 'Protocollo',
                es: 'Protocolo',
                pl: 'Protokół',
                uk: 'Протокол',
                'zh-cn': '协议',
            },
            type: 'string',
            role: 'info.firmware',
            read: true,
            write: false,
            def: '',
        });

        // Topics channel (stores resolved topic config for reference and re-use after restart)
        await this._ensureChannel(`${this.deviceFolder}.topics`, {
            en: 'MQTT Topics',
            de: 'MQTT-Topics',
            ru: 'MQTT-топики',
            pt: 'Tópicos MQTT',
            nl: 'MQTT-topics',
            fr: 'Topics MQTT',
            it: 'Topic MQTT',
            es: 'Topics MQTT',
            pl: 'Tematy MQTT',
            uk: 'MQTT-топіки',
            'zh-cn': 'MQTT 主题',
        });
        await this._ensureState(`${this.deviceFolder}.topics.state`, {
            name: {
                en: 'State topic prefix',
                de: 'Status-Topic-Präfix',
                ru: 'Префикс топика состояния',
                pt: 'Prefixo do tópico de estado',
                nl: 'Voorvoegsel statustopic',
                fr: "Préfixe du topic d'état",
                it: 'Prefisso topic stato',
                es: 'Prefijo del topic de estado',
                pl: 'Prefiks tematu stanu',
                uk: 'Префікс топіка стану',
                'zh-cn': '状态主题前缀',
            },
            type: 'string',
            role: 'text',
            read: true,
            write: false,
            def: topicConfig.topicstate,
        });
        await this._ensureState(`${this.deviceFolder}.topics.command`, {
            name: {
                en: 'Command topic prefix',
                de: 'Kommando-Topic-Präfix',
                ru: 'Префикс топика команд',
                pt: 'Prefixo do tópico de comando',
                nl: 'Voorvoegsel opdrachttopic',
                fr: 'Préfixe du topic de commande',
                it: 'Prefisso topic comando',
                es: 'Prefijo del topic de comando',
                pl: 'Prefiks tematu poleceń',
                uk: 'Префікс топіка команд',
                'zh-cn': '命令主题前缀',
            },
            type: 'string',
            role: 'text',
            read: true,
            write: false,
            def: topicConfig.topiccommand,
        });
        await this._ensureState(`${this.deviceFolder}.topics.setting`, {
            name: {
                en: 'Setting topic prefix',
                de: 'Einstellungs-Topic-Präfix',
                ru: 'Префикс топика настроек',
                pt: 'Prefixo do tópico de configuração',
                nl: 'Voorvoegsel instellingstopic',
                fr: 'Préfixe du topic de paramètre',
                it: 'Prefisso topic impostazioni',
                es: 'Prefijo del topic de ajuste',
                pl: 'Prefiks tematu ustawień',
                uk: 'Префікс топіка налаштувань',
                'zh-cn': '设置主题前缀',
            },
            type: 'string',
            role: 'text',
            read: true,
            write: false,
            def: topicConfig.topicsetting,
        });
        await this._ensureState(`${this.deviceFolder}.topics.prefixApp`, {
            name: {
                en: 'Prefix app name',
                de: 'Präfix App-Name',
                ru: 'Префикс имени приложения',
                pt: 'Prefixo do nome do aplicativo',
                nl: 'Voorvoegsel app-naam',
                fr: "Préfixe du nom de l'application",
                it: 'Prefisso nome app',
                es: 'Prefijo nombre de app',
                pl: 'Prefiks nazwy aplikacji',
                uk: 'Префікс назви програми',
                'zh-cn': '前缀应用名称',
            },
            type: 'boolean',
            role: 'indicator',
            read: true,
            write: false,
            def: topicConfig.prefixapp,
        });
        await this._ensureState(`${this.deviceFolder}.topics.prefixHost`, {
            name: {
                en: 'Prefix host name',
                de: 'Präfix Host-Name',
                ru: 'Префикс имени хоста',
                pt: 'Prefixo do nome do host',
                nl: 'Voorvoegsel hostnaam',
                fr: "Préfixe du nom d'hôte",
                it: 'Prefisso nome host',
                es: 'Prefijo nombre de host',
                pl: 'Prefiks nazwy hosta',
                uk: 'Префікс імені хоста',
                'zh-cn': '前缀主机名',
            },
            type: 'boolean',
            role: 'indicator',
            read: true,
            write: false,
            def: topicConfig.prefixhost,
        });

        // Data states from FIELDS
        const hasBle = activeFields.has('ble') || activeFields.has('ble_temp');
        let bleChannelCreated = false;

        for (const [key, def] of Object.entries(FIELDS)) {
            if (def.bleKey) {
                // BLE fields are only created if the device reports BLE
                if (!hasBle) {
                    continue;
                }
                // autob is a string BLE identifier, not a sensor sub-field
                const isBleSubField = key.startsWith('ble_');
                if (isBleSubField) {
                    if (!bleChannelCreated) {
                        await this._ensureChannel(`${this.deviceFolder}.ble`, {
                            en: 'BLE Sensor',
                            de: 'BLE-Sensor',
                            ru: 'BLE-сенсор',
                            pt: 'Sensor BLE',
                            nl: 'BLE-sensor',
                            fr: 'Capteur BLE',
                            it: 'Sensore BLE',
                            es: 'Sensor BLE',
                            pl: 'Czujnik BLE',
                            uk: 'BLE-сенсор',
                            'zh-cn': 'BLE 传感器',
                        });
                        bleChannelCreated = true;
                    }
                    const subKey = key.slice(4); // ble_temp -> temp
                    await this._ensureFieldState(`${this.deviceFolder}.ble.${subKey}`, def);
                } else {
                    // autob: flat field under device folder
                    await this._ensureFieldState(`${this.deviceFolder}.${key}`, def);
                }
                continue;
            }

            // Regular fields: only create if active on device
            if (!activeFields.has(key)) {
                continue;
            }

            await this._ensureFieldState(`${this.deviceFolder}.${key}`, def);
        }

        // Config states (user-writable settings forwarded to device via MQTT)
        if (createConfigStates) {
            await this._ensureChannel(`${this.deviceFolder}.config`, {
                en: 'Device Configuration',
                de: 'Gerätekonfiguration',
                ru: 'Конфигурация устройства',
                pt: 'Configuração do dispositivo',
                nl: 'Apparaatconfiguratie',
                fr: "Configuration de l'appareil",
                it: 'Configurazione dispositivo',
                es: 'Configuración del dispositivo',
                pl: 'Konfiguracja urządzenia',
                uk: 'Конфігурація пристрою',
                'zh-cn': '设备配置',
            });
            for (const [key, def] of Object.entries(CONFIG_FIELDS_DEF)) {
                await this._ensureFieldState(`${this.deviceFolder}.config.${key}`, def);
            }
        }
    }

    // Parse MQTT status JSON and update states
    async updateFromMqttPayload(payload: string): Promise<void> {
        let json: Record<string, unknown>;
        try {
            json = JSON.parse(payload) as Record<string, unknown>;
        } catch (e) {
            this.adapter.log.warn(`${this.deviceFolder}: Failed to parse MQTT payload: ${String(e)}`);
            return;
        }

        for (const [key, def] of Object.entries(FIELDS)) {
            if (def.writeOnly) {
                continue;
            }
            if (def.bleKey && key.startsWith('ble_')) {
                continue;
            } // handled via ble sub-object

            const rawVal = json[key];
            if (rawVal === undefined || rawVal === null) {
                continue;
            }

            const converted = this._convertReadValue(key, def, rawVal);
            if (converted === null || converted === undefined) {
                continue;
            }

            await this.adapter.setStateAsync(`${this.deviceFolder}.${key}`, { val: converted, ack: true });
        }

        // BLE sub-fields from json.ble
        const bleObj = json.ble;
        if (bleObj !== null && bleObj !== undefined && typeof bleObj === 'object') {
            const ble = bleObj as Record<string, unknown>;
            const bleMap: Record<string, string> = {
                ble_temp: 'temp',
                ble_hum: 'hum',
                ble_bat: 'bat',
                ble_volt: 'volt',
            };
            for (const [fieldKey, subKey] of Object.entries(bleMap)) {
                const rawVal = ble[subKey];
                if (rawVal === undefined || rawVal === null) {
                    continue;
                }
                const def = FIELDS[fieldKey];
                if (!def) {
                    continue;
                }
                const converted = this._convertReadValue(fieldKey, def, rawVal);
                if (converted === null || converted === undefined) {
                    continue;
                }
                await this.adapter.setStateAsync(`${this.deviceFolder}.ble.${subKey}`, { val: converted, ack: true });
            }
        }

        // autob (BLE MAC reference) - string from top-level json
        if (json.autob !== undefined && json.autob !== null) {
            await this.adapter.setStateAsync(`${this.deviceFolder}.autob`, {
                val: String(json.autob),
                ack: true,
            });
        }

        // protocol – store in info, then update fan states map
        if (json.protocol !== undefined && json.protocol !== null) {
            const protocol = String(json.protocol);
            await this.adapter.setStateAsync(`${this.deviceFolder}.info.protocol`, { val: protocol, ack: true });
            await this._updateFanStates(protocol);
        }

        // Update lastUpdate timestamp
        await this.adapter.setStateAsync(`${this.deviceFolder}.info.lastUpdate`, { val: Date.now(), ack: true });
    }

    // Update fan state's allowed values based on fantype + protocol
    private async _updateFanStates(protocol: string): Promise<void> {
        const fantypeState = await this.adapter.getStateAsync(`${this.deviceFolder}.info.fantype`);
        const fantype = fantypeState?.val != null ? Number(fantypeState.val) : 0;
        const states = getFanStates(fantype, protocol);
        await this.adapter.extendObjectAsync(`${this.deviceFolder}.fan`, { common: { states } });
    }

    // Check if a state has been marked as user-modified (native.userModified = true)
    isUserModified(key: string): boolean {
        // Synchronous check is not straightforward; this is a marker for future use.
        // In Phase 1 we always allow updates unless the user manually sets the flag.
        void key;
        return false;
    }

    // Remove states that are no longer part of the device profile
    async removeStaleStates(profile: DeviceProfile): Promise<void> {
        const hasBle = profile.activeFields.has('ble') || profile.activeFields.has('ble_temp');

        for (const key of Object.keys(FIELDS)) {
            const def = FIELDS[key];
            if (!def) {
                continue;
            }

            const isBleField = def.bleKey && key.startsWith('ble_');
            const statePath = isBleField ? `${this.deviceFolder}.ble.${key.slice(4)}` : `${this.deviceFolder}.${key}`;

            const shouldExist = isBleField ? hasBle : def.bleKey ? hasBle : profile.activeFields.has(key);

            if (!shouldExist) {
                try {
                    const existing = await this.adapter.getObjectAsync(statePath);
                    if (existing) {
                        const custom = (existing.common as ioBroker.StateCommon).custom;
                        if (custom && Object.keys(custom).length > 0) {
                            this.adapter.log.warn(
                                `Stale state ${statePath} has custom data (History/InfluxDB) – skipping deletion to preserve it`,
                            );
                        } else {
                            await this.adapter.delObjectAsync(statePath);
                            this.adapter.log.debug(`Removed stale state: ${statePath}`);
                        }
                    }
                } catch {
                    // Object does not exist - no problem
                }
            }
        }
    }

    // Build a state common object from a FieldDef
    private _buildStateCommon(def: FieldDef): ioBroker.StateCommon {
        const common: ioBroker.StateCommon = {
            name: def.label as ioBroker.StringOrTranslated,
            type: def.type,
            role: def.role ?? (def.type === 'boolean' ? (def.writeTopic ? 'switch' : 'indicator') : 'value'),
            read: !def.writeOnly,
            write: !!def.writeTopic || !!def.forceWrite,
        };
        if (def.unit) {
            common.unit = def.unit;
        }
        if (def.states) {
            common.states = def.states;
        }
        if (def.min !== undefined) {
            common.min = def.min;
        }
        if (def.max !== undefined) {
            common.max = def.max;
        }
        return common;
    }

    private async _ensureFieldState(id: string, def: FieldDef): Promise<void> {
        const existing = await this.adapter.getObjectAsync(id);
        if (existing?.native?.userModified === true) {
            this.adapter.log.debug(`Skipping userModified state: ${id}`);
            return;
        }
        // extendObjectAsync merges into the existing object, preserving common.custom
        // (History/InfluxDB settings). Falls back to creation if the object does not exist yet.
        await this.adapter.extendObjectAsync(id, {
            type: 'state',
            common: this._buildStateCommon(def),
            native: { userModified: false },
        });
    }

    private async _ensureState(
        id: string,
        common: ioBroker.StateCommon & {
            def?: ioBroker.StateValue;
        },
    ): Promise<void> {
        const { def: defaultVal, ...rest } = common;
        await this.adapter.extendObjectAsync(id, {
            type: 'state',
            common: rest,
            native: { userModified: false },
        });
        if (defaultVal !== undefined) {
            const existing = await this.adapter.getStateAsync(id);
            if (existing === null || existing === undefined) {
                await this.adapter.setStateAsync(id, { val: defaultVal, ack: true });
            }
        }
    }

    private async _ensureChannel(id: string, name: string | Record<string, string>): Promise<void> {
        await this.adapter.setObjectNotExistsAsync(id, {
            type: 'channel',
            common: { name: name as ioBroker.StringOrTranslated },
            native: {},
        });
    }

    // Convert a raw JSON value to the correct TypeScript type for a field
    private _convertReadValue(key: string, def: FieldDef, rawVal: unknown): ioBroker.StateValue {
        if (rawVal === null || rawVal === undefined) {
            return null;
        }

        switch (def.type) {
            case 'boolean':
                return rawVal === true || rawVal === 1 || rawVal === '1' || rawVal === 'true';
            case 'number': {
                // Integer fields
                if (key === 'demand' || key === 'Whheating' || key === 'Whcooling' || key === 'Whoutside') {
                    return parseInt(String(rawVal), 10);
                }
                return parseFloat(String(rawVal));
            }
            case 'string':
                return String(rawVal);
            default:
                return String(rawVal);
        }
    }
}
