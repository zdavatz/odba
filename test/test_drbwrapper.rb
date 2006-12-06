#!/usr/bin/env ruby
# TestDRbWrapper -- odba -- 13.06.2006 -- hwyss@ywesee.com


$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'test/unit'
require 'odba/drbwrapper'
require 'flexmock'

module ODBA
  class TestDRbWrapper < Test::Unit::TestCase
    def test_include
      obj = FlexMock.new
      obj2 = FlexMock.new
      arr = [obj]
      assert_equal(false, arr.include?(obj2))
      assert_equal(true, arr.include?(DRbWrapper.new(obj)))
      assert_equal(false, arr.include?(DRbWrapper.new(obj2)))
      arr = [DRbWrapper.new(obj)]
      assert_equal(true, arr.include?(DRbWrapper.new(obj)))
      assert_equal(false, arr.include?(DRbWrapper.new(obj2)))
    end
  end
end
