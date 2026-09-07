# Changelog

## [1.1.6](https://github.com/amir20/drain/compare/v1.1.5...v1.1.6) (2026-09-07)

### Performance Improvements

* **dashboard:** cache the analytics endpoints against the refresh timestamp ([1af13f0](https://github.com/amir20/drain/commit/1af13f0fa56b2fb6215cad0f8e343cc1bf5f25e0))

## [1.1.5](https://github.com/amir20/drain/compare/v1.1.4...v1.1.5) (2026-09-07)

### Bug Fixes

* **dashboard:** render before the data arrives, fit the header on a phone, build once ([5f10d0a](https://github.com/amir20/drain/commit/5f10d0a67a9b7fb16c4e0878b355270cccaff1c0))

## [1.1.4](https://github.com/amir20/drain/compare/v1.1.3...v1.1.4) (2026-09-07)

### Bug Fixes

* **deploy:** prune services that are no longer in the compose files ([7e6723f](https://github.com/amir20/drain/commit/7e6723f83dad6fc964553281abdd537c811fab41))

### Performance Improvements

* **dashboard:** stop rescanning and row-multiplying on Features and Environment ([88fcab7](https://github.com/amir20/drain/commit/88fcab722445271a4c6291520ae2998dd4ec26bd))

## [1.1.3](https://github.com/amir20/drain/compare/v1.1.2...v1.1.3) (2026-09-07)

### Features

* **dashboard:** replace Grafana with a Nuxt analytics dashboard ([542d55b](https://github.com/amir20/drain/commit/542d55bdc8f1d0910bd72242155b41a337fc6cd8))
* **stack:** schedule the refresh in TimescaleDB, deliver credentials as Docker secrets ([1c6df6b](https://github.com/amir20/drain/commit/1c6df6bca023883a25645f06755e4405fb625ffc))

### Bug Fixes

* **dashboard:** correctness and legibility fixes from review ([4903d17](https://github.com/amir20/drain/commit/4903d177f07bc947759b18f88a4a8ac7202254b1))

## [1.1.2](https://github.com/amir20/drain/compare/v1.1.1...v1.1.2) (2026-09-07)

### Bug Fixes

* **deploy:** version the postgres init config name ([942c96e](https://github.com/amir20/drain/commit/942c96e1cd5a259dc49671c9f84380d1d45ad41e))

## [1.1.1](https://github.com/amir20/drain/compare/v1.1.0...v1.1.1) (2026-09-07)

### Bug Fixes

* **deploy:** keep swarm config names under the 64-character limit ([f1daa04](https://github.com/amir20/drain/commit/f1daa04ce7e1aff279118d279550f126bb78c231))

## [1.1.0](https://github.com/amir20/drain/compare/v1.0.21...v1.1.0) (2026-09-07)

### Features

* **dashboard:** faster loads, correct stickiness, and descriptive analytics ([4e0066d](https://github.com/amir20/drain/commit/4e0066d139be8f2c0416307185a1a43eebda3660))
* **grafana:** provisioned retention, usage and feature dashboards ([75b15a1](https://github.com/amir20/drain/commit/75b15a1eb03f1d6791ab60362b1bc11d8c26e932))

### Bug Fixes

* **deps:** update all non-major dependencies ([27d393a](https://github.com/amir20/drain/commit/27d393a087325c37d5c516a055285c5ee74b6896))
* **deps:** update all non-major dependencies ([3d4a956](https://github.com/amir20/drain/commit/3d4a956ac030cf7dea667ce8bd6d7a549c312208))
* **deps:** update all non-major dependencies ([539f919](https://github.com/amir20/drain/commit/539f919d07cbc9a8690a21ba1b3dbef5d6065e1e))
* **deps:** update all non-major dependencies ([da533de](https://github.com/amir20/drain/commit/da533deba6b3ac2df5f7111e584886f745d1635f))
* **deps:** update all non-major dependencies ([#100](https://github.com/amir20/drain/issues/100)) ([b86b22e](https://github.com/amir20/drain/commit/b86b22eb58e00a9c795ee1f80b3aedd3dd64cba0))
* **deps:** update all non-major dependencies ([#102](https://github.com/amir20/drain/issues/102)) ([10f679f](https://github.com/amir20/drain/commit/10f679fe820bbe28292ae9ff1aa94479f3e80250))
* **deps:** update all non-major dependencies ([#103](https://github.com/amir20/drain/issues/103)) ([ba49ad8](https://github.com/amir20/drain/commit/ba49ad8d34768195f003768f12a4645a34947ab9))
* **deps:** update all non-major dependencies ([#105](https://github.com/amir20/drain/issues/105)) ([3cb4555](https://github.com/amir20/drain/commit/3cb45552b2ece5a6da090ee1798591e7fd6e347c))
* **deps:** update all non-major dependencies ([#106](https://github.com/amir20/drain/issues/106)) ([8f88dd6](https://github.com/amir20/drain/commit/8f88dd637a5fa85f503a362c1d4437749adbb315))
* **deps:** update all non-major dependencies ([#107](https://github.com/amir20/drain/issues/107)) ([87fab85](https://github.com/amir20/drain/commit/87fab85b2305ab2f2e9f8ef0a4c89c9bef163b98))
* **deps:** update all non-major dependencies ([#109](https://github.com/amir20/drain/issues/109)) ([92f9c4d](https://github.com/amir20/drain/commit/92f9c4d7f5c5e845075f87a3015cd34d4c0ba117))
* **deps:** update all non-major dependencies ([#110](https://github.com/amir20/drain/issues/110)) ([4eb0684](https://github.com/amir20/drain/commit/4eb0684c092de7d7762f80a1a1124d4bd39610f8))
* **deps:** update all non-major dependencies ([#111](https://github.com/amir20/drain/issues/111)) ([350f386](https://github.com/amir20/drain/commit/350f3864107f5a92a94655b10f16db58db9e4952))
* **deps:** update all non-major dependencies ([#112](https://github.com/amir20/drain/issues/112)) ([ea0c420](https://github.com/amir20/drain/commit/ea0c42016d06ce38f16a04611bd25573b9c7cf4e))
* **deps:** update all non-major dependencies ([#114](https://github.com/amir20/drain/issues/114)) ([2af16d9](https://github.com/amir20/drain/commit/2af16d9b5210c0f120495625a2551dcfe09ea450))
* **deps:** update all non-major dependencies ([#118](https://github.com/amir20/drain/issues/118)) ([40a9775](https://github.com/amir20/drain/commit/40a9775639705606574e719a9dba2e39658df2ac))
* **deps:** update all non-major dependencies ([#120](https://github.com/amir20/drain/issues/120)) ([0c59f26](https://github.com/amir20/drain/commit/0c59f269afbdbd6d9d5be20e591e4cf178cc0a9b))
* **deps:** update all non-major dependencies ([#121](https://github.com/amir20/drain/issues/121)) ([d01e8a5](https://github.com/amir20/drain/commit/d01e8a5f99b42b886b04fd4935134d0e214de8b1))
* **deps:** update all non-major dependencies ([#122](https://github.com/amir20/drain/issues/122)) ([ddfc01b](https://github.com/amir20/drain/commit/ddfc01b7b3dc968578b14932dbc21993313affd2))
* **deps:** update all non-major dependencies ([#125](https://github.com/amir20/drain/issues/125)) ([5d1308c](https://github.com/amir20/drain/commit/5d1308cb731b1ea92b2acd65a6876a0b94e7795e))
* **deps:** update all non-major dependencies ([#127](https://github.com/amir20/drain/issues/127)) ([59c520b](https://github.com/amir20/drain/commit/59c520b52af663051b6c8f9a06d202cf335c3eec))
* **deps:** update all non-major dependencies ([#131](https://github.com/amir20/drain/issues/131)) ([4bddec8](https://github.com/amir20/drain/commit/4bddec84df2515e2f827bca00df1bb3fda3308f6))
* **deps:** update all non-major dependencies ([#133](https://github.com/amir20/drain/issues/133)) ([e3d1cf7](https://github.com/amir20/drain/commit/e3d1cf7de1da2b5a8abf45907f218862c3186ed8))
* **deps:** update all non-major dependencies ([#134](https://github.com/amir20/drain/issues/134)) ([7ae2de9](https://github.com/amir20/drain/commit/7ae2de95619aa9f52203da4a1e70a5b908dbaaef))
* **deps:** update all non-major dependencies ([#64](https://github.com/amir20/drain/issues/64)) ([08d74af](https://github.com/amir20/drain/commit/08d74af06091d20d623448fb68ae35489f7bb1a0))
* **deps:** update all non-major dependencies ([#65](https://github.com/amir20/drain/issues/65)) ([cb427c5](https://github.com/amir20/drain/commit/cb427c509e6dfa9e51fd8b2b8bbc7f4542149870))
* **deps:** update all non-major dependencies ([#67](https://github.com/amir20/drain/issues/67)) ([8357c4a](https://github.com/amir20/drain/commit/8357c4a1cec3bca796da36e995823a3477cd7c8a))
* **deps:** update all non-major dependencies ([#69](https://github.com/amir20/drain/issues/69)) ([3394e0f](https://github.com/amir20/drain/commit/3394e0f13df5b12cd2a9b4ed643ef79c8a31aa0c))
* **deps:** update all non-major dependencies ([#70](https://github.com/amir20/drain/issues/70)) ([af74e8f](https://github.com/amir20/drain/commit/af74e8f192bd6565c0afa72048b86d2264f5442f))
* **deps:** update all non-major dependencies ([#72](https://github.com/amir20/drain/issues/72)) ([4e0cf3f](https://github.com/amir20/drain/commit/4e0cf3f4181332c734d2b6a8a00a576ea8a7d1e9))
* **deps:** update all non-major dependencies ([#73](https://github.com/amir20/drain/issues/73)) ([98d2dfd](https://github.com/amir20/drain/commit/98d2dfddb868e521ea5a9b001ebde2222fb6d48e))
* **deps:** update all non-major dependencies ([#74](https://github.com/amir20/drain/issues/74)) ([b74c8c7](https://github.com/amir20/drain/commit/b74c8c780dd82ae446c5ae4e7a927d167a4a609b))
* **deps:** update all non-major dependencies ([#75](https://github.com/amir20/drain/issues/75)) ([950e0b1](https://github.com/amir20/drain/commit/950e0b17c0b517f83cb223f613971d419eaed9c3))
* **deps:** update all non-major dependencies ([#76](https://github.com/amir20/drain/issues/76)) ([27e2378](https://github.com/amir20/drain/commit/27e2378357d6c7dcbe4a6dd21c8f02f0c71a6665))
* **deps:** update all non-major dependencies ([#80](https://github.com/amir20/drain/issues/80)) ([845d48b](https://github.com/amir20/drain/commit/845d48b71e52e3dded1d89400af246d14192becf))
* **deps:** update all non-major dependencies ([#81](https://github.com/amir20/drain/issues/81)) ([8a37243](https://github.com/amir20/drain/commit/8a37243f32f46154d88d85ca9e1db4593e2c0ee2))
* **deps:** update all non-major dependencies ([#83](https://github.com/amir20/drain/issues/83)) ([5618c0a](https://github.com/amir20/drain/commit/5618c0ae41e562d7ba552acf6ec5a69764b167eb))
* **deps:** update all non-major dependencies ([#85](https://github.com/amir20/drain/issues/85)) ([bc4e137](https://github.com/amir20/drain/commit/bc4e13779cfd2a747fa1e98d8e021dd42becef74))
* **deps:** update all non-major dependencies ([#86](https://github.com/amir20/drain/issues/86)) ([1775f7e](https://github.com/amir20/drain/commit/1775f7ea2b059883dfd7d8451eb17ed01194c6ae))
* **deps:** update all non-major dependencies ([#90](https://github.com/amir20/drain/issues/90)) ([6b46e03](https://github.com/amir20/drain/commit/6b46e030812bd2d4bf2acd1847984abef21f98bf))
* **deps:** update all non-major dependencies ([#91](https://github.com/amir20/drain/issues/91)) ([11d4e04](https://github.com/amir20/drain/commit/11d4e041be76d2706d1eb9de7c0ef71c42b4893b))
* **deps:** update all non-major dependencies ([#92](https://github.com/amir20/drain/issues/92)) ([999b59a](https://github.com/amir20/drain/commit/999b59afac54852819192bb35bbd812a6b609959))
* **deps:** update all non-major dependencies ([#94](https://github.com/amir20/drain/issues/94)) ([f78edc4](https://github.com/amir20/drain/commit/f78edc44db352826d72552bab9b377c14d282d4e))
* **deps:** update all non-major dependencies ([#98](https://github.com/amir20/drain/issues/98)) ([22ccdf1](https://github.com/amir20/drain/commit/22ccdf117b41e8f2b5e5f8d56811a48d188161f4))
* **deps:** update all non-major dependencies ([#99](https://github.com/amir20/drain/issues/99)) ([7bf488d](https://github.com/amir20/drain/commit/7bf488db09b9854dc0957095dec138144e0dd1fb))
* **deps:** update dependency plotly to v7 ([b7b4cb0](https://github.com/amir20/drain/commit/b7b4cb071d47d096bfe6b76c23e455002d4ace1d))
* **deps:** update dependency pyarrow to v23 ([31c08ac](https://github.com/amir20/drain/commit/31c08ac657a7653dac4c56506afbc55d714d99b2))
* **deps:** update dependency pyarrow to v24 ([79178f4](https://github.com/amir20/drain/commit/79178f47377f4e07d255c31475f32b8905585b80))
* **deps:** update dependency pyarrow to v25 ([85db22d](https://github.com/amir20/drain/commit/85db22d60e86782738b66651fea1ad0375d68c31))
* **deps:** update dependency ruff to >=0.12.10 ([#79](https://github.com/amir20/drain/issues/79)) ([84e4659](https://github.com/amir20/drain/commit/84e46591ab0343b7d74f3eb06a6d24c1968b3d3d))
* **deps:** update dependency ruff to >=0.12.2 ([#71](https://github.com/amir20/drain/issues/71)) ([68a1028](https://github.com/amir20/drain/commit/68a1028f9b2ec3c65c3981b30107577db05a7004))
* **deps:** update dependency ruff to >=0.13.1 ([#82](https://github.com/amir20/drain/issues/82)) ([96d99c3](https://github.com/amir20/drain/commit/96d99c3e415d3d337d99d0956ee77cff625b6718))
* **deps:** update dependency ruff to >=0.14.5 ([#93](https://github.com/amir20/drain/issues/93)) ([09c1e2a](https://github.com/amir20/drain/commit/09c1e2a1f6386a52065528e85052965fbdca4889))
* **deps:** update dependency ruff to >=0.14.7 ([#96](https://github.com/amir20/drain/issues/96)) ([84b5e3f](https://github.com/amir20/drain/commit/84b5e3f4a6a6d1f7526b3df69b4bef0694da5522))
* **deps:** update dependency ruff to >=0.15.8 ([#119](https://github.com/amir20/drain/issues/119)) ([18a68fa](https://github.com/amir20/drain/commit/18a68fa907b2bf219ae163463d99ae9c2f83c9ef))
* **deps:** update module github.com/parquet-go/parquet-go to v0.26.4 ([#101](https://github.com/amir20/drain/issues/101)) ([00256a9](https://github.com/amir20/drain/commit/00256a9ed8e984449aea7cb07c351706918f484e))
* **notebooks:** drop casts made redundant by polars 1.44 ([2fbaa62](https://github.com/amir20/drain/commit/2fbaa6203adb4daacea24535a591a28e09c82665))
* **notebooks:** resolve ruff lint errors ([32ad8e4](https://github.com/amir20/drain/commit/32ad8e4768c197ae8a83f5bf1a281f65475b89dc))
