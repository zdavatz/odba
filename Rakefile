#!/usr/bin/env ruby
# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'odba/version'
require "bundler/gem_tasks"
require 'rake/testtask'

# dependencies are now declared in odba.gemspec

desc 'Offer a gem task like hoe'
task :gem => :build do
  Rake::Task[:build].invoke
end

desc "Run tests"
task :default => :test

desc 'Run odba with all commonly used combinations'
task :test do
  log_file = 'suite.log'
  res = system("bash -c 'set -o pipefail && bundle exec ruby test/suite.rb 2>&1 | tee #{log_file}'")
  puts "Running test/suite.rb returned #{res.inspect}. Output was redirected to #{log_file}"
  exit 1 unless res
end

require 'rake/clean'
CLEAN.include FileList['pkg/*.gem']

# vim: syntax=ruby
