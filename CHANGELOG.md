# Changelog

## 1.0.0-a4

* Initial release - thanks [dtabuenc]
* Port to Devart driver - thanks [jeffreywilbur]

## 1.0.0-a5

* Fix table name comparison - thanks [martywang]

## 1.0.0-a7

* Change saga data type from raw to blob - thanks [gonzoga]

## 2.0.0-a5

* Update to new Oracle driver and target .NET Standard 2.0 - thanks [gonzoga]
* Add ability to enlist in `TransactionScope` - thanks [thomasdc]
* Use synchronous API of Oracle driver, because it's not even async - thanks [jods4]
* Add databus storage - thanks [jods4]
* Refactor connection management, reduce allocations, query string caching, + more - thanks [jods4]

---

[dtabuenc]: https://github.com/dtabuenc
[gonzoga]: https://github.com/gonzoga
[jeffreywilbur]: https://github.com/jeffreywilbur
[jods4]: https://github.com/jods4
[martywang]: https://github.com/martywang
[thomasdc]: https://github.com/thomasdc