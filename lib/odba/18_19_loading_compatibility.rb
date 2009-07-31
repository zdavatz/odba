#!/usr/bin/env ruby

require 'date'
require 'strscan'

if RUBY_VERSION >= '1.9'
  def u str
    str
  end
  class Date
    def self._load(str)
      scn = StringScanner.new str
      a = []
      while match = scn.get_byte
        case match
        when ":"
          len = scn.get_byte
          name = scn.scan /.{#{Marshal.load("\x04\bi#{len}")}}/
        when "i"
          int = scn.get_byte
          size, = int.unpack('c')
          if size > 1 && size < 5
            size.times do 
              int << scn.get_byte
            end
          end
          dump = "\x04\bi" << int
          a.push Marshal.load(dump)
        end
      end

      ajd = of = sg = 0
      if a.size == 3
        num, den, sg = a
        ajd = Rational(num,den)
        ajd -= 1.to_r/2
      else
        num, den, of, sg = a
        ajd = Rational(num,den)
      end
      new!(ajd, of, sg)
    end
  end
  class Encoding
    class Character
      class UTF8 < String
        module Methods
        end
        ## when loading Encoding::Character::UTF8 instances simply return
        #  an encoded String
        def self._load data
          str = Marshal.load(data)
          str.force_encoding 'UTF-8'
          str
        end
      end
    end
  end
else
  class Date
    def marshal_load a
      @ajd, @of, @sg, = a
      @__ca__ = {}
    end
  end
  class Rational
    def marshal_load a
      @numerator, @denominator, = a
    end
  end
end
