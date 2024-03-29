# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.8] - 2023-03-22
### Added
- Add fastparquet to virtual environment
### Changed 
- execution_environment version up to 1.2.3

## [1.4.7] - 2023-03-07
### Changed
- Changed default logger name to exec_env
- Set strict scipy version 

## [1.4.6] - 2023-02-16
### Fixed
- fix raising casting error when read parquet

## [1.4.5] - 2023-02-02
### Fixed
- Fix conda environment build

## [1.4.4] - 2023-01-31
### Fixed
- Fix raising error with empty values and lists in column
### Added
- Add statsmodels to virtual environment

- scipy version >= 1.9.3
## [1.4.3] - 2022-12-14
### Changed
- scipy version >= 1.9.3

## [1.4.2] - 2022-12-07
### Fixed 
- Fix error with spark DECIMAL

## [1.4.1] - 2022-11-30
### Added
- Added platform_envs dictionary to command

## [1.4.0] - 2022-11-22
### Fixed 
- Fix field name in ddl might be an integer
### Added
- scikit-learn library to environment
- xgboost library to environment

## [1.3.1] - 2022-10-25
### Fixed
- Error with schema when initial type is ARRAY

## [1.3.0] - 2022-07-26
### Added
- Support for Boolean type
- Support for Timestamp type

## [1.2.1] - 2022-07-06
### Changed
- OTLang version bump

## [1.2.0] - 2022-07-06
### Fixed
- Small typo in CHANGELOG.md
### Changed
- Changed `make clean` behaviour to clean only distribution files
- Completely redesigned Jenkins pipelines
### Added
- Support for PyPi publishing

## [1.1.0] - 2022-05-27
### Changed
- Replaced default value passed to `transform` (`None`) with an empty `pd.DataFrame`
- Replaced `Syntax` with a new implementation from `execution_environment:1.2.0`
- Dropped requirements.txt in favor of build_environment.yml

## [1.0.6] - 2022-05-04
### Changed
- Logging of command configuration stage

## [1.0.5] - 2022-04-13
### Added
- `otlang` as a requirement
### Changed
- Bumped OTLang version
- Replaced `Rule` with new alternatives from `otlang.sdk` in tests
- Changed `rules` type-hint in `BaseCommand` to encourage the use of `otlang.sdk`

## [1.0.4] - 2022-03-31
### Fixed
- Fixed prepare.sh not working from another directory

## [1.0.3] - 2022-03-31
### Fixed
- `venv` not working in the context of Python Computing Node

## [1.0.2] - 2022-03-30
### Added
- External libraries required by Python Computing Node

## [1.0.1] - 2022-03-30
### Fixed
- prepare.sh not working

## [1.0.0] - 2022-03-29
### Added
- Initial release
- `BaseCommand` class for command developers
- Implementation of all required abstractions
