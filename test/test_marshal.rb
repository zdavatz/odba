#!/usr/bin/env ruby

$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'test/unit'
require 'odba/marshal'

module ODBA
	class TestMarshal < Test::Unit::TestCase
		def setup
			@foo = Array.new
		end
		def test_dump
			assert_equal("04085b00",ODBA::Marshal.dump(@foo))
		end
		def test_load
			assert_equal(@foo, ODBA::Marshal.load("04085b00"))
		end
    def test_load_18_in_19
      if RUBY_VERSION >= '1.9'
        require 'odba/18_19_loading_compatibility'
        binary = "\004\bu:\tDate=\004\b[\bo:\rRational\a:\017@numeratori\003\205\353J:\021@denominatori\ai\000i\003\031\025#".unpack('H*').first
        date = Marshal.load(binary)
        assert_equal Date.new(2009,5,27), date
      end
    end
	end
end
