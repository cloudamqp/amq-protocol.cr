# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.15] - 2025-05-20

### Added

- Table#hash, copy the implemenation from Hash#hash, which produces the same hash regardless of order of keys

## [1.1.14] - 2024-02-17

### Added

- Frame#from_io without a block, buffering the whole frame instead. Only makes a difference for body frames, which can be streamed with the block version but not with this new one.

## [1.1.13] - 2024-02-13

### Added

- Connection#UpdateSecret/Ok frame definition

## [1.1.12] - 2023-12-07

### Fixed

- Fixed an issue where Table#merge with NamedTuple failed to replace the old value and instead just added another entry for the same key.

### Changed

- Table#delete and Table#has_key? now only accepts String argument. Other types will issue a deprecation warning.

## [1.1.11] - 2023-09-28

### Added

- Add support for Table to be merged with another Table

## [1.1.10] - 2023-09-22

### Fixed

- Fixed an issue where reassigning values in a Table with a nested Table would overwrite existing data.

## [1.1.9] - 2023-08-22

### Fixed

- Fixed issue where copying directly to the buffer didn't update the position in the IO, causing subsequent writes to overwrite existing data.

## [1.1.8] - 2023-08-22

### Changed

- Write Hash/NamedTuple directly to Table IO

## [1.1.7] - 2023-08-22

### Changed

- Optimize Table performance with copy-on-write

### Added

- Table#reject!/#merge! support

### Fixed

- Support negative (signed) values for the Decimal values in Table

## [1.1.6] - 2023-07-20

### Fixed

- Validate property flags also when "skipping" Properties.

## [1.1.5] - 2023-06-27

### Added

- Frame#to_slice, returns a byte slice of the frame (but uses heap allocation underneath)

## [1.1.4] - 2023-05-04

### Fixed

Update @bytesize in Frames when properties (such as queue name) are updated

## [1.1.3] - 2023-03-04

### Fixed

- Optimized Properties#to_json
- Return early from Properties.from_bytes/from_io/#bytesize if flags == 0
- Table#to_io is now thread safe

## [1.1.2] - 2022-11-26

### Changed

- Include type # in FrameDecode error message if invalid Method type

## [1.1.1] - 2022-11-18

### Fixed

- Bug fix for Properties.from_json

## [1.1.0] - 2022-11-18

### Changed

- Removed Properties.cast_to_fields

## [1.0.8] - 2022-11-14

### Fixed

- Removed the use of a StringPool in ShortStrings, to fix a multi threading issue and memory growth issues.

## [1.0.7] - 2022-10-28

### Changed

- 10-20% faster encoding/decoding of frames

## [1.0.6] - 2022-02-22

### Added

- Support for JSON::Any in Tables

## [1.0.5] - 2022-02-03

### Changed

- Table.from_bytes copies the bytes, so that it can be modified later on

## [1.0.4] - 2022-02-01

### Added

- Properties/ShortString/Table.from_bytes for parsing directly from a byte slice

## [1.0.3] - 2022-02-01

### Fixed

- Properties#timestamp= now works as expected

### Added

- Try parse timestamp as milliseconds from epoch if seconds from epoch overflows

## [1.0.2] - 2022-02-01

### Changed

- Parse timestamp on demand, as it might not contain a valid timestamp

## [1.0.1] - 2021-06-09

### Added

- Table can be created from a NamedTuple

## [1.0.0] - 2021-03-23

### Changed

- Crystal 1.0.0 compability

## [0.3.24] - 2021-01-26

### Fixed

- Crystal 0.36.0 compability

## [0.3.23] - 2021-01-25

### Fixed

- Exchange::UnbindOk did not have the wrong method id, only wireshark had it backwards

## [0.3.22] - 2021-01-25

### Fixed

- Exchange::UnbindOk had the wrong method id

## [0.3.21] - 2020-11-27

### Fixed

- Tx::Select was handled as Confirm::Select

## [0.3.20] - 2020-10-05

### Changed

- Table's are now compared on a semantic level, not on the byte level
- Table#to_h is now recursive, in the sense that lower level Tables are also converted to hashes

## [0.3.19] - 2020-09-27

### Added

- Support for parsing Decimal numbers (to Float64) in Tables

## [0.3.18] - 2020-09-23

### Changed

- Queue::Bind#routing_key is a property so that it can be overwritten

## [0.3.17] - 2020-09-22

### Changed

- Queue name is now a property on Consume/Get frames too

## [0.3.16] - 2020-09-22

### Added

- Support for Transaction frames

### Changed

- Queue name is now a property on all frames that includes that argument

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
