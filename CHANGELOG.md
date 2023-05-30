# Changelog
## 0.1.2
## 0.1.3
  * Dependabot update [#27](https://github.com/singer-io/tap-snapchat-ads/pull/27)

  * Fall back to default values for un-configured optional params [#25](https://github.com/singer-io/tap-snapchat-ads/pull/25)
  * Fixes following issues [#26](https://github.com/singer-io/tap-snapchat-ads/pull/26)
    * `End time should be after start time` error
    *  URL creation using `urlencode`

## 0.1.1
  * Added mechanism to extract data for selected profiles [#22](https://github.com/singer-io/tap-snapchat-ads/pull/22)

## 0.1.0
  * Dict based to class based refactoring [#13](https://github.com/singer-io/tap-snapchat-ads/pull/13)
  * Add retry mechanism and error logging [#14](https://github.com/singer-io/tap-snapchat-ads/pull/14)
  * Fix bookmark strategy [#16](https://github.com/singer-io/tap-snapchat-ads/pull/16)
  * Fix replication key for the roles stream [#17](https://github.com/singer-io/tap-snapchat-ads/pull/17)
  * Fix record write for parent of grand child stream [#19](https://github.com/singer-io/tap-snapchat-ads/pull/19)
  * Add tap-tester tests for better test coverage [#15] (https://github.com/singer-io/tap-snapchat-ads/pull/15)
  * Implement Request Timeout [#12] (https://github.com/singer-io/tap-snapchat-ads/pull/12)

## 0.0.3
  * Always send the timezone as UTC [#5](https://github.com/singer-io/tap-snapchat-ads/pull/5)

## 0.0.2
  * Adjust bookmarking strategy in `sync.py`. Incorporate code-review feedback. Add `tap-tester` tests and `.circleci`.

## 0.0.1
  * Initial commit