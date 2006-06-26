#!/usr/bin/env ruby
# DRbWrapper -- ydim -- 11.01.2006 -- hwyss@ywesee.com

require 'drb'
require 'odba'
require 'drb/timeridconv'

module ODBA
  class DRbIdConv < DRb::TimerIdConv
    def to_obj(ref)
      if(ref.is_a?(String))
        ODBA.cache.fetch(ref.to_i)
      else
        super
      end
    rescue RuntimeError => e
      raise RangeError, e.message
    end
    def to_id(obj)
      if(!obj.is_a?(ODBA::Persistable) || obj.odba_unsaved?)
        super
      else
        obj.odba_id.to_s
      end
    end
  end
  class DRbWrapper 
    instance_methods.each { |m| 
      undef_method(m) unless m =~ /^(__)|(respond_to\?$)/ }
    include DRb::DRbUndumped
    def initialize(obj)
      @obj = obj
    end
    def respond_to?(sym)
      super || @obj.respond_to?(sym)
    end
    def method_missing(sym, *args)
      if(block_given?)
        res = @obj.__send__(sym, *args) { |*block_args|
          yield *block_args.collect { |arg| __wrap(arg) }
        }
        __wrap(res)
      else
        res = @obj.__send__(sym, *args)
        if(res.is_a?(Array))
          res.collect { |item| __wrap(item) }
        else
          __wrap(res)
        end
      end
    end
    def __wrap(obj)
      if(obj.is_a?(ODBA::Persistable))
        DRbWrapper.new(obj.odba_instance)
      else
        obj
      end
    end
    def __wrappee
      @obj
    end
  end
end
