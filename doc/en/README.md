# ioBroker.faikout-ac

Adapter for the [Faikout](https://codeberg.org/RaW07/faikout-projects) Daikin MQTT bridge.

The Faikout is an ESP32-based device that connects to a Daikin air conditioner via the CN_WIRED port and exposes control and status via MQTT (RevK MQTT schema). This adapter subscribes to the Faikout status topic and maps the JSON payload to individual ioBroker states.

---

## Prerequisites

- ioBroker with js-controller >= 6.0.0 and admin >= 7.0.0
- An MQTT broker accessible from ioBroker (e.g. Mosquitto)
- One or more Faikout devices on the same network, connected to the MQTT broker and delivering data

---

## Installation

Install the adapter via ioBroker Admin:

1. Open ioBroker Admin
2. Go to **Adapters**
3. Search for `faikout`
4. Click **Install**

---

## Configuration

After installation, open the adapter instance settings.

### Tab: Devices

Add one entry per Faikout device:

| Field | Description |
|---|---|
| Enabled | Enable or disable this device without removing it |
| Name | Folder name under `faikout-ac.0.*`. Only letters, numbers, underscore and dash. |
| IP Address | Local IP address of the Faikout device |

After saving and restarting the adapter, it will perform HTTP discovery on each device and create all states automatically.

### Tab: MQTT Broker

| Field | Description |
|---|---|
| Broker host | Hostname or IP of the MQTT broker (default: `localhost`) |
| Broker port | Port (default: `1883`) |
| Username | Optional MQTT username |
| Password | Optional MQTT password (stored encrypted) |

### Tab: Options

| Field | Description |
|---|---|
| Create config states | Creates a `config/` sub-folder with write-only states for Faikout settings (see note below) |
| env_set cycle interval | How often (in seconds) the adapter re-sends the room temperature to the Faikout (default: 30 s) |

---

## State Structure

After discovery, states are created under `faikout.0.{Name}/`:

```
faikout-ac.0.
+-- info.
|   +-- connection          boolean (global MQTT connection status)
|
+-- {Name}/
    +-- info/
    |   +-- deviceId        string (MAC/hostname of the Faikout)
    |   +-- appName         string (firmware app name)
    |   +-- ip              string (IP address)
    |   +-- connection      boolean (per-device MQTT status)
    |   +-- lastUpdate      number (Unix timestamp of last MQTT message)
    |
    +-- topics/
    |   +-- state           string
    |   +-- command         string
    |   +-- setting         string
    |   +-- prefixApp       boolean
    |   +-- prefixHost      boolean
    |
    +-- power               boolean rw
    +-- mode                string  rw  (H=Heat, C=Cool, A=Auto, D=Dry, F=Fan)
    +-- fan                 string  rw  (A/1/2/3/4/5/Q)
    +-- temp                number  ro  (Daikin internal setpoint, degrees C)
    +-- swingv              boolean rw
    +-- swingh              boolean rw
    +-- econo               boolean rw
    +-- powerful            boolean rw
    +-- comfort             boolean rw
    +-- quiet               boolean rw
    +-- streamer            boolean rw  (only if active on device)
    +-- led                 boolean rw  (only if active on device)
    +-- demand              number  rw  (0-100 %)
    +-- humidify            string  rw  (only if active)
    +-- autoe               boolean rw
    +-- autop               boolean rw
    +-- autot               number  rw  (target temperature, degrees C)
    +-- autor               number  rw  (hysteresis, degrees C)
    +-- auto0               string  rw  (timer off time, format HH:MM)
    +-- auto1               string  rw  (timer on time, format HH:MM)
    +-- env_set             number  wo  (send room temperature to Faikout)
    +-- restart             boolean wo  (write true to restart device)
    +-- home                number  ro  (indoor temperature, degrees C)
    +-- outside             number  ro  (outdoor temperature, degrees C)
    +-- liquid              number  ro  (refrigerant temperature, degrees C)
    +-- inlet               number  ro  (inlet temperature, degrees C)
    +-- env                 number  ro  (room temperature from Faikout, degrees C)
    +-- hum                 number  ro  (humidity, %)
    +-- comp                number  ro  (compressor speed, Hz)
    +-- fanrpm              number  ro  (fan speed, RPM)
    +-- consumption         number  ro  (power consumption, W)
    +-- Whheating           number  ro  (heating energy cumulative, Wh)
    +-- Whcooling           number  ro  (cooling energy cumulative, Wh)
    +-- Whoutside           number  ro  (outdoor unit energy, Wh)
    +-- anglev              number  ro  (vertical vane angle, degrees)
    +-- angleh              number  ro  (horizontal vane angle, degrees)
    +-- antifreeze          boolean ro
    +-- autob               string  ro  (BLE reference device MAC, only if BLE active)
    |
    +-- ble/                (only if BLE sensor active)
    |   +-- temp            number  ro  (degrees C)
    |   +-- hum             number  ro  (%)
    |   +-- bat             number  ro  (%)
    |   +-- volt            number  ro  (V)
    |
    +-- config/             (only if "Create config states" is enabled)
        +-- heatback        number  wo  (heat pull-back, degrees C)
        +-- heatover        number  wo  (heat push-over, degrees C)
        +-- coolback        number  wo  (cool pull-back, degrees C)
        +-- coolover        number  wo  (cool push-over, degrees C)
        +-- tcontrol        number  wo  (control message timeout, s)
        +-- thermref        number  wo  (inlet/home blend ratio, %)
        +-- tmin            number  wo  (min temperature limit, degrees C)
        +-- tmax            number  wo  (max temperature limit, degrees C)
        +-- tcoolmin        number  wo  (min temp limit cooling, degrees C)
        +-- theatmax        number  wo  (max temp limit heating, degrees C)
        +-- frosttemp       number  wo  (frost protection, degrees C)
        +-- minoutside      number  wo  (min outdoor temperature, degrees C)
        +-- switchtemp      number  wo  (switch-point offset, degrees C)
        +-- pushtemp        number  wo  (push-point offset, degrees C)
        +-- reporting       number  wo  (status report interval, s)
        +-- lockmode        boolean wo  (lock heat/cool mode)
```

---

## Note: Write-Only Config States

When "Create config states" is enabled, a `config/` sub-folder is created under each device. These states are **write-only**. The Faikout does not include configuration parameters in its MQTT status messages, so ioBroker cannot read back the current values.

The state always shows the last value written by ioBroker. If you change a setting via the Faikout web UI, the ioBroker state will be out of sync. Use the Faikout web UI (`http://{ip}/`) as the authoritative source for the current configuration.

---

## Note: Multiple Instances

All devices configured in one adapter instance share a single MQTT broker connection. If you need to connect to two different MQTT brokers, create a second adapter instance (`faikout-ac.1`).

---

## Phase 1 Limitations

- Writing to states (commands to the Faikout) is not yet implemented. This is planned for Phase 2.
- OLA GPIO mod (xgpio states) is planned for Phase 5.
- Schema-topic-based discovery is planned for Phase 4.

---

## License

MIT
