# ioBroker.faikout-ac

![Version](https://img.shields.io/github/package-json/v/RaW79/ioBroker.faikout-ac)
![Status](https://img.shields.io/badge/status-beta-orange)
![License](https://img.shields.io/badge/license-MIT-green)
<p align="right"><a href="https://buymeacoffee.com/raw07" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" height="40" alt="Buy Me A Coffee"></a></p>

ioBroker adapter for **Faikout** – the ESP32-based Daikin controller by [RevK](https://github.com/revk/ESP32-Faikout).

The adapter connects to one or more Faikout devices via MQTT and automatically detects which features each device supports. All corresponding states are created and kept up to date via MQTT push. No manual topic or state configuration required. Sensors, control commands and device parameters are available directly in ioBroker for use in scripts, visualisations and automations.

> **Beta.** Successfully tested with **single-split** and **multi-split** Daikin devices. Feedback and issues welcome.

## Design Principles

The adapter communicates with Faikout devices through their native interfaces: MQTT for real-time data and partly HTTP for device discovery. **There is no dependency on implementation details of any other platform.** The MQTT broker already running in the network is used directly, no additional infrastructure required. The Faikout device remains accessible to other automation platforms at the same time, in the spirit of an open platform approach rather than a device-specific standalone solution.

## Demand Control

In heating mode the adapter automatically adjusts the `demand` value (outdoor unit power limit) based on outdoor temperature (weather-compensated control curve). Optional extensions:

- **Boost:** increases demand step by step when room temperature falls below setpoint
- **Taper:** reduces demand linearly when room temperature exceeds setpoint
- **Guard:** overheating protection, switches to a higher heatback value when the guard threshold is reached

Result: steady compressor operation instead of cycling and full-load peaks.

![Demand Control Configuration](doc/demand-control.png)

## Humidity Control (Dew Point)

The adapter calculates the dew point from room temperature and humidity (Faikout BLE sensor or any ioBroker state). If the margin between room temperature and dew point is too small, or relative humidity is too high, it switches the device to dehumidification mode (**Dry**) automatically. Once the margin is sufficient again, a fan run-on follows to dry the heat exchanger.

- Configurable start and stop thresholds (dew point margin in K, maximum humidity in %)
- Time window, maximum runtime with forced pause and run-on time
- Detects manual overrides on the device and monitors sensor data freshness

![Humidity Control Configuration](doc/feuchteregelung.png)

## Multi-Split Support

Multiple indoor units on one outdoor unit can be grouped together. Demand control calculates independently per indoor unit (own room temperature, own curve), but uses the highest minimum demand across all group members as a floor. This prevents the outdoor unit from being throttled too much while another unit still needs heating output. The outdoor temperature sensor is shared within the group.

## Further Features

- Automatic device discovery (only active fields are created as states)
- Multiple Faikout devices per adapter instance
- Schedule: weekday-based switching times per device with multiple actions (power, fan, powerful, setpoint, manual demand value)
- External temperature sensor: any ioBroker state is forwarded cyclically as room temperature (`env`) to the Faikout device

## Requirements

- ioBroker with js-controller >= 6.0.11 and Admin >= 7.6.17, Node.js >= 22
- MQTT broker (e.g. Mosquitto) reachable by ioBroker
- Faikout device(s) in the local network publishing to that broker

## Installation

Install via ioBroker Admin:

1. ioBroker Admin -> **Adapters** -> search for `faikout` -> click **Install**
2. Add a **Faikout** instance (`faikout-ac.0`)

## Setup

1. Tab **MQTT Connection**: enter broker host, port and credentials
2. Tab **Devices**: add one row per Faikout device (name + IP address); enable **Demand Control** and/or **Humidity Control** checkboxes per device as needed; for multi-split systems assign all indoor units of the same outdoor unit to the same group (A–E)
3. Save, then click **Discover** for each device
4. States appear under `faikout-ac.0.<Name>.*`

## Changelog

### 0.1.992
- Fix MQTT client-ID uniqueness (host-scoped), foreign state read for env sensor, lockmode state sync, English log messages

### 0.1.991
- Fix German string in discovery callback

### 0.1.99
- Fix device connection role (indicator.reachable), consumption role (value.power), preserve common.custom on object update, remove direct install link from doc

### 0.1.98
- Fix env_set state: read/write flags, role level

### 0.1.97
- Maintenance release

### 0.1.96
- Fix state roles (fan, mode, env_set, info.connection), update existing objects on upgrade

### 0.1.94
- Fix object structure: missing humidity_ctrl channel, read/write flags on config states, 11-language state names

### 0.1.93
- Fix CI test setup for repochecker compliance

### 0.1.92
- Fix CI test setup for repochecker compliance

### 0.1.91
- Switch to npm trusted publishing (OIDC)

### 0.1.9
- Fix npm OIDC trusted publishing

### 0.1.8
- Switch to npm trusted publishing (OIDC)

### 0.1.7
- Responsive layout fixes for all admin tabs, installation instructions updated

### 0.1.6
- Repochecker fixes: multilingual translations (11 languages), node:http, this.setTimeout, prettier config

### 0.1.5
- Schedule modes for grouped activation/deactivation of schedule entries
- Adapter title fix (Faikout AC)
- Language detection fix via socket

### 0.1.4
- Full bilingual UI (EN/DE) for all admin tabs
- CI pipeline (Node 22/24/26 on Ubuntu/Windows/macOS)

## License

MIT License

Copyright (c) 2026 RWS <RWS@nurfuerspam.de>

See [LICENSE](LICENSE) file for details.
