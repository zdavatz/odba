#!/usr/bin/env ruby
# IndexDefinition -- odba -- 20.09.2004 -- hwyss@ywesee.com

require 'yaml'

module ODBA
	class IndexDefinition
		attr_accessor :index_name, :dictionary, :origin_klass, 
			:target_klass, :resolve_search_term, :resolve_target, 
			:resolve_origin, :fulltext, :init_source
		def initialize
			@index_name = ""
			@origin_klass = ""
			@target_klass = ""
			@resolve_search_term = ""
			@resolve_target = ""
			@resolve_origin = ""
			@dictionary = ""
			@init_source = ""
			@fulltext = false
		end
	end
end
