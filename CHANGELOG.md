## [Unreleased][unreleased]
#### Added

#### Fixed

## [0.5-7] - 2015/05/05
#### Added
- Accepting multiple hosts with different ports when connecting (#55)

## [0.5-6] - 2015/04/19
#### Fixed
- Accessing nil rows in auto_paging (#50)

## [0.5-5] - 2015/03/28
#### Added
- Expose error code (#46)

#### Fixed
- Fix documentation for set_keepalive()

## [0.5-4] - 2015/03/04
#### Added
- Better travis configuration (coverage, lua 5.1, 5.2 and luajit)

#### Fixed
- `auto_pagination` option not returning the latest page in most cases

## [0.5-3] - 2015/03/01
#### Fixed
- Tests (file structure)

## [0.5-2] - 2015/02/24
#### Fixed
- Installation (missing rockspec files)

## [0.5] - 2015/02/24
#### Added
- Support for pagination.
- Support for batch types.
- Better, complete documentation.
- The `version` property is now effective.

#### Fixed
- Batch statement queries without parameters
- Require contact_points to not be nil #39
- Seed random number generator only once, on module import

## [0.4] - 2015/02/06
#### Added
- Batch support (#7).
- Smarter session creation: choose between cosocket and luasocket on each new session (#29).

## [0.3] - 2015/01/22
#### Added
- Allow result rows access by name or position index (#27).
- More explicit error messages.

#### Fixed
- Query tracing.

## [0.2] - 2014/07/28
#### Added
- Choose a random contact point from nodes list (#18).
- Add support for tracing on write (#2).

#### Fixed
- Calls to `setkeepalive` and `getreusedtimes` while using luasocket now return an error.

## 0.1 - 2014-07-26
- First release.

[unreleased]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-7...HEAD
[0.5-7]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-6...v0.5-7
[0.5-6]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-5...v0.5-6
[0.5-5]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-4...v0.5-5
[0.5-4]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-3...v0.5-4
[0.5-3]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5-2...v0.5-3
[0.5-2]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.5...v0.5-2
[0.5]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.4...v0.5
[0.4]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.3...v0.4
[0.3]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.2...v0.3
[0.2]: https://github.com/jbochi/lua-resty-cassandra/compare/v0.1...v0.2
