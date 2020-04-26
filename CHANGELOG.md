# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.8] - 2020-04-26

### Fixed
- Properties.skip handles headers properly
- Explicit return types on impl of abstract methods

## [0.3.7] - 2020-04-26

### Added
- AMQ::Protocol::Properties.skip method

## [0.3.6] - 2020-04-07

### Fixed
- Don't try catch Errno that's removed in Crystal 0.34.0

## [0.3.5] - 2020-03-04

### Changed
- The initial capacity of the string pool in ShortString is increased to 256 entries

## [0.3.4] - 2020-01-17

### Added
- Clone methods for Properties and Table

## [0.3.3] - 2019-10-21

### Added
- Queue::Bind#queue_name is now a property (as opposed to a reader)
- Frame#inspect

## [0.3.2] - 2019-09-11

### Fixed
- GetEmpty parsed correctly (short-string argument instead of UInt16)

## [0.3.1] - 2019-09-11

### Fixed
- Timestamp is correctly parsed as seconds from unix epoch

## [0.3.0] - 2019-07-10

### Added
- Headers are now parsed on-demand instead of everytime

## [0.2.6] - 2019-06-17

### Added
- Adding all parsed Short Strings to a StringPool to decrease GC pressure
- Added this CHANGELOG file
