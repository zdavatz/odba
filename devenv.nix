{
  inputs,
  pkgs,
  config,
  lib,
  ...
}:
{

  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.libyaml
    pkgs.nixfmt-rfc-style
  ];

  enterShell = ''
    echo This is the devenv shell for odba_test
    git --version
    ruby --version
    psql --version
    bundle install
  '';
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    listen_addresses = "0.0.0.0";
    port = 5432;

    initialDatabases = [
      { name = "odba_test"; }
    ];

    initdbArgs = [
      "--locale=C"
      "--encoding=UTF8"
    ];

    initialScript = ''
      create role odba_test superuser login password null;
    '';
  };
  languages.ruby.enable = true;
  languages.ruby.version = "3.4";
  # See full reference at https://devenv.sh/reference/options/
  enterTest = ''
    # No `devenv up` here: `devenv test` already starts the processes before
    # running enterTest, so starting them again aborts the whole run with
    # "Processes already running with PID ...".
    #
    # Those processes come up in parallel with this script. On slower runners
    # initdb is still creating template0 when we arrive and the first connect
    # is refused, so wait for postgres to accept connections. The bare
    # pg_isready afterwards turns "never came up" into a clear error rather
    # than a confusing PG::ConnectionBad from the test.
    for _ in $(seq 1 60); do
      if pg_isready --host 127.0.0.1 --port 5432 > /dev/null 2>&1; then break; fi
      sleep 1
    done
    pg_isready --host 127.0.0.1 --port 5432
    bundle exec ruby test/example.rb
  '';
}
