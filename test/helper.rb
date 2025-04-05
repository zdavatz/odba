$: << File.dirname(__FILE__)
$: << File.expand_path("../lib/", File.dirname(__FILE__))
require "test/unit"
require "flexmock/test_unit"
require 'simplecov'
SimpleCov.start 'test_frameworks'
