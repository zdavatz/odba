#!/usr/bin/env ruby
require_relative "helper"
require "odba/marshal"

module ODBA
  class TestMarshal < Test::Unit::TestCase
    def setup
      @foo = []
    end

    def teardown
      super
    end

    def test_dump
      assert_equal("04085b00", ODBA::Marshal.dump(@foo))
    end

    def test_load
      assert_equal(@foo, ODBA::Marshal.load("04085b00"))
    end
  end
end
