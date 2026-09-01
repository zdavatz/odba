## 1.2.3 / 01.09.2026

1.2.2 narrowed the rescue around the peer notification in `Cache#next_id` too
far. "A peer we cannot reach" is not only `DRb::DRbError`: a reference into a
peer that has restarted, or whose entry has expired from the DRb object space,
raises `RangeError("invalid reference")` from `DRbObjectSpace#to_obj` - a
StandardError, and not a DRbError. 1.1.9's classless rescue absorbed it; 1.2.2
let it out and it reached the caller.

In oddb.org that showed up on the first night under 1.2.2: three index
rebuilds died with `invalid reference (druby://127.0.0.1:10000)`. An index
that is not built is a deferred index, and ODBA fills a deferred index in
`Cache#setup` - so the *next* process to start died on it too, before doing
any work of its own.

* `next_id` re-raises `OdbaDuplicateIdError` (the one exception that must
  reach the retry, which is what 1.2.2 was for) and swallows every other
  StandardError from a peer, as 1.1.9 did.
* Two regression tests, one per direction: a stale reference must not stop
  the allocation, and the catch-all must not swallow the conflict again.
  The first fails against 1.2.2 with the production error.

## 1.2.2 / 31.08.2026

Two processes on one database handed out the same odba_id. Whichever wrote
last overwrote the other's row in `object`, and every reference to the lost
object then resolved to a foreign one - an Array where a domain object
belonged, or the reverse. The referring instance variable stays correct and
points at the right number; a different object simply sits under it, so
searching the application for the offending assignment finds nothing.

* `Storage#next_id` takes the id from a Postgres sequence, `odba_id_seq`,
  which `#setup` creates. It used to be `@next_id += 1` under a mutex, with
  @next_id seeded once per process from the highest odba_id in the table -
  sound for one process, wrong for every deployment running a web worker and
  an import job against the same database. Measured with two processes side
  by side, both answered `[61935067, 61935068, 61935069]`. Stores without the
  sequence keep the old behaviour.
* The sequence starts at `MAX(odba_id)` plus `ID_SEQUENCE_GAP`, not at 1: a
  plain `CREATE SEQUENCE` would re-issue ids that already exist, and the gap
  covers ids that processes still on the old counter hold but have not
  written yet.
* `Cache#next_id` no longer swallows `OdbaDuplicateIdError`. The guard was
  there all along - a peer raises it when the id is taken and the method
  retries - but `rescue` without a class caught it too, so the retry could
  never run. Only `DRb::DRbError` is caught now, which is what the line was
  for: an unreachable peer must not stop the allocation.

## 1.2.1 / 21.08.2026

No library changes: lib/ is identical to 1.2.0. This release only ships a test
suite and CI that pass.

* Cleared CacheEntry's @@id_table between cache tests. Its ObjectSpace
  finalizer calls ODBA.cache.invalidate, so objects left by earlier tests were
  collected at unpredictable moments and deleted ids the running test had just
  stored - the long standing flakiness in test_clean, test_clean__prefetched
  and test_transaction
* Fixed test_to_obj for drb >= 2.2, which resolves only ids to_id handed out
  rather than looking them up with ObjectSpace._id2ref
* Bumped install-nix-action to v31: v26 cannot provision Nix build users on
  current macOS images
* Stopped starting the processes twice in enterTest and waited for postgres
  before running test/example.rb
* Declared the git-hooks input that current devenv no longer injects implicitly
* Dropped a continue-on-error that used matrix.ruby.to_s, which GitHub Actions
  expressions do not support, so it silently never applied

## 1.2.0 / 21.08.2026

* Reconnect when the database connection was lost. ydbd-pg reports a lost
  connection as DBI::ProgrammingError, which the ConnectionPool excluded from
  its retry, so a restart of the PostgreSQL server left every pooled connection
  broken until the client process itself was restarted
* Removed obsolete install.rb. Updated History.txt and moved it to History.md
* Used standarb for all files
* Removed obsolete (and not needed) WITH_OIDS to allow running with postgres 12 and later
* Use SimpleCov. Simplified test/test*.rb to include test/helper.rb and start coverage
* Updated README.md for devenv
* Run test/exmple.rb using devenv in .github/workflows/devenv.yml
* Added test/exmple.rb using a real Postgresql 17. Updated README
* Merged Guide.txt and into README.md
* Silence NOTICE if table already exists
* Removed obsolete Manifest.txt and 18_19_loading_compatibility
* Added devenv environment for Ruby 3.4 and Postgresql 17
* limit github CI to rubies 3.2,3.3 and 3.4
* changelog_uri to gemspec

## 1.1.9 / 28.06.2025

* bulk_restore throws OdbaError when stack size > 1000

## 1.1.8 / 10.03.2021

* Removed fixed dependency to pg (is optional like mysql2)

## 1.1.7 / 20.01.2021

* Reworked some tests
* Updated to use Ruby 3.0.0
* Added github actions
* Updated to use ydbi 0.5.7

## 1.1.6 / 23.01.2016

* Updated to use ydbi 0.5.6

## 1.1.5 / 23.01.2016

* Remove unused parameter dict for update_fulltext_index

## 1.1.4 / 13.12.2017

* Drop text search dictionaries/configuration before recreating
* Remove dictionary argument in fulltext search and index_definition

## 1.1.3 / 12.12.2016

* Avoid errors by always specifying "IF NOT EXISTS" when creating tables and indices
* Add utility method get_server_version
* Removed misleading check in generate_dictionary

## 1.1.2 / 10.05.2016

* requires now 'ydbi' and 'ydbd-pg'

## 1.1.1 / 10.05.2016

* Made tests pass under Ruby 1.9.3, 2.x
* Updated to use minitest

## 1.1.0 / 15.03.2013

* Update dict source file name for multi languages

## 1.0.9 / 15.03.2013

* Update dictionary to be suitable for new tsearch2
  - generate_dictionary API is changed.
  - generate_dictionary needs dict source files(dict, affix, stop)
    into /usr/share/postgresql/tsearch_data.

## 1.0.8 / 09.01.2012

* Added exclusive control to update @accessed_by Hash variable in CacheEntry class using mutex (Patinfo-Invoice Error)

## 1.0.7 / 27.12.2011

* Debugged Hash iteration error during cleaning @fetched and @prefetched objects

## 1.0.6 / 23.12.2011

* Fix all the elements of odba_potentials and odba_exclude_vars to be Symbols for Ruby 1.9.3

## 1.0.5 / 20.12.2011

* Added attr_accessor :odba_persistent so we can update doctor addresses on ch.oddb.org

## 1.0.4 / 12.12.2011

* Fixed file lock process by using @file_lock flag variable to control it in application side

## 1.0.3 / 09.12.2011

* Debugged ODBA::Cache#next_id

## 1.0.2 / 09.12.2011

* Updated cache.rb persitatble.rb 18_19_loading_compatibility.rb to be compatible for both Ruby 1.8 and 1.9

## 1.0.1 / 08.12.2011

* Added file lock exclusive control to create a new odba_id between processes.

## 1.0.0 / 20.12.2010

* Add ODBA.cache.index_matches(index_name, substring)

  * this new method returns all search-terms of a given index (identified by index_name) that start with substring.
