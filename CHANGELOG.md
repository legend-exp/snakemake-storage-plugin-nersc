# Changelog

## [0.1.2](https://github.com/legend-exp/snakemake-storage-plugin-nersc/compare/v0.1.1...v0.1.2) (2025-12-15)


### Bug Fixes

* add missing setting to publish workflow ([f5b5925](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/f5b592596fa07857bcbee15b7bb080fd72cf3fae))

## [0.1.1](https://github.com/legend-exp/snakemake-storage-plugin-nersc/compare/v0.1.0...v0.1.1) (2025-12-15)


### Bug Fixes

* re-add conventional prs workflow ([60a9d77](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/60a9d77e42b6bac5ad3eecf02380799d7acfced5))

## 0.1.0 (2025-12-15)


### Features

* register nersc storage plugin entry point in pyproject ([92c32f5](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/92c32f5fcb40158e9220446be898b127ca294ae3))
* use read-only root for globbing via physical_ro_root setting ([f75079a](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/f75079a9725e86687d00c089283b932328df062f))


### Bug Fixes

* align storage plugin with updated interface APIs ([5735270](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/5735270616b9e677adc0228eaf51ea76f28c5d9e))
* implement local filesystem storage provider and tests ([976c70b](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/976c70bedcd9406935c4a25160a25ea5d3e6762a))
* implement StorageObject cleanup and inventory methods ([669a409](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/669a40989f6a4f9c1634b25c3f0c850c1ef1a020))
* make storage plugin read-only and update tests accordingly ([9fc4abb](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/9fc4abb28faee3adfbd8fd29fc66d1362a6b19e6))
* make storage roots configurable and tests portable ([358a99f](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/358a99fb8dab74f4785d954e6ff8c26984553ace))
* relax inventory to avoid IOCache set_exists usage ([024971b](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/024971b56826ce11e6f96b3d21b74360019fc35a))
* remove unused Path import from test_plugin ([46d7d00](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/46d7d005bc0a24f811aff935ae7c916620f78fe0))
* support test overrides for NERSC path mapping ([7453881](https://github.com/legend-exp/snakemake-storage-plugin-nersc/commit/74538817f509aa6433732cfed0ea1cf60e50b09c))
