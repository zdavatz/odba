require 'yaml'
module ODBA
	class IndexDefinition
		attr_accessor :index_name, :dictionary, :origin_klass, :target_klass, :resolve_search_term, :resolve_target, :resolve_origin, :fulltext
		def initialize
			@index_name = ""
			@origin_klass = ""
			@target_klass = ""
			@resolve_search_term = ""
			@resolve_target = ""
			@resolve_origin = ""
			@dictionary = ""
			@fulltext = false
		end
	end
end
