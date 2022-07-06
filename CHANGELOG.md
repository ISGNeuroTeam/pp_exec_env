# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
