= odba

* http://scm.ywesee.com/?p=odba/.git;a=summary

== DESCRIPTION:

Object Database Access - Ruby Software for ODDB.org Memory Management

== FEATURES/PROBLEMS:

* There may still be an sql bottleneck. Has to be investigated further.
* You will need postgresql installed.

== IMPORTANT

* You should NOT install dbd-pg-0.3.x for the postgreSQL driver and
  dbi-0.4.x, because it has already been known that the dbd-pg-0.3.x
  depends on the library, deprecated-0.2.x, that causes an error.

== INSTALL:

* gem install odba

== DEVELOPERS:

* Masamoi Hatakeyama
* Zeno R.R. Davatz
* Hannes Wyss (up to Version 1.0)

== LICENSE:

* GPLv2.1
