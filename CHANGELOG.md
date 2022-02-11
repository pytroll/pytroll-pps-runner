## Version <v0.2.4> (2022/02/11)


### Issues Closed

* [Issue 41](https://github.com/pytroll/pytroll-pps-runner/issues/41) - level1c runner read orbit_number from posttroll message ([PR 42](https://github.com/pytroll/pytroll-pps-runner/pull/42) by [@TAlonglong](https://github.com/TAlonglong))
* [Issue 34](https://github.com/pytroll/pytroll-pps-runner/issues/34) - pps hook publisher need configuration option for nameservers ([PR 36](https://github.com/pytroll/pytroll-pps-runner/pull/36) by [@TAlonglong](https://github.com/TAlonglong))
* [Issue 27](https://github.com/pytroll/pytroll-pps-runner/issues/27) - Add a general level1c runner to the repo ([PR 29](https://github.com/pytroll/pytroll-pps-runner/pull/29) by [@adybbroe](https://github.com/adybbroe))

In this release 3 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 49](https://github.com/pytroll/pytroll-pps-runner/pull/49) - Bugfix xml timestat from ascii

#### Features added

* [PR 42](https://github.com/pytroll/pytroll-pps-runner/pull/42) - Use orbit number from message. ([41](https://github.com/pytroll/pytroll-pps-runner/issues/41))
* [PR 36](https://github.com/pytroll/pytroll-pps-runner/pull/36) - Add nameservers as metadata options ([34](https://github.com/pytroll/pytroll-pps-runner/issues/34))
* [PR 31](https://github.com/pytroll/pytroll-pps-runner/pull/31) - Add nameserver config option in pps2018-runner
* [PR 30](https://github.com/pytroll/pytroll-pps-runner/pull/30) - Make publish topic for pps posttroll hook patternable
* [PR 29](https://github.com/pytroll/pytroll-pps-runner/pull/29) - Add pps2021 support ([27](https://github.com/pytroll/pytroll-pps-runner/issues/27))
* [PR 28](https://github.com/pytroll/pytroll-pps-runner/pull/28) - Add level1c runner

In this release 7 pull requests were closed.


## Version <v0.2.1> (2021/05/07)

### Pull Requests Merged

#### Bugs fixed

* [PR 23](https://github.com/pytroll/pytroll-pps-runner/pull/23) - Fix granule duration check, to allow shorter than 48 scans in a granule

#### Features added

* [PR 26](https://github.com/pytroll/pytroll-pps-runner/pull/26) - Fix the integration with Coveralls
* [PR 25](https://github.com/pytroll/pytroll-pps-runner/pull/25) - Fix consistent publish topic or specify in yaml file
* [PR 24](https://github.com/pytroll/pytroll-pps-runner/pull/24) - Raise time2wait in publish thread


In this release 4 pull requests were closed.

## Version <v0.2.0> (2020/12/08)

### Issues Closed

* [Issue 14](https://github.com/pytroll/pytroll-pps-runner/issues/14) - Add unittests
* [Issue 12](https://github.com/pytroll/pytroll-pps-runner/issues/12) - Add ci support

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 18](https://github.com/pytroll/pytroll-pps-runner/pull/18) - Fix bug 'tmp_orb'
* [PR 15](https://github.com/pytroll/pytroll-pps-runner/pull/15) - Fix missing metadata in messages
* [PR 7](https://github.com/pytroll/pytroll-pps-runner/pull/7) - Bring recent metno/python3 updates inline with requirements at smhi

#### Features added

* [PR 20](https://github.com/pytroll/pytroll-pps-runner/pull/20) - Fix pygrib openfiles
* [PR 19](https://github.com/pytroll/pytroll-pps-runner/pull/19) - Add github templates and travis support
* [PR 17](https://github.com/pytroll/pytroll-pps-runner/pull/17) - Refactor the pps posttroll messaging hook and add unittests
* [PR 15](https://github.com/pytroll/pytroll-pps-runner/pull/15) - Fix missing metadata in messages
* [PR 13](https://github.com/pytroll/pytroll-pps-runner/pull/13) - Fix version and package naming
* [PR 9](https://github.com/pytroll/pytroll-pps-runner/pull/9) - Add runner for seviri
* [PR 7](https://github.com/pytroll/pytroll-pps-runner/pull/7) - Bring recent metno/python3 updates inline with requirements at smhi

In this release 10 pull requests were closed.
