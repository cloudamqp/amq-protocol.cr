# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.15] - 2020-09-20

### Fixed
- Raise Error::FrameDecode when invalid Property flags are detected

### Added
- Frames::Header#class_id and #weight are now exposed

### Changed
- Error::FrameDecode is now raised on unexpected Frame type

## [0.3.14] - 2020-06-27

### Changed
- BytesBody where the body is a slice of bytes instead of an IO object

## [0.3.13] - 2020-06-21

### Changed
- Crystal 0.35.1 compability, don't return bytes on to_io

## [0.3.12] - 2020-06-11

### Changed
- Frame#to_io returns number of bytes written

## [0.3.11] - 2020-06-10

### Fixed
- Crystal 0.35 compability

## [0.3.10] - 2020-05-29

### Changed
- Internal byteformat of Table is always NetworkEndian, but allow prefix size to be in any format
- No default byteformat for Properties.skip

## [0.3.9] - 2020-04-28

### Fixed
- Properties.skip didn't skip the length byte(s)

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
