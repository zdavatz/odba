#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'

module ODBA
	module Persistable
		attr_accessor :odba_references
		attr_writer :odba_id
		public :odba_replace_excluded!
	end
	class StubMock < Mock
		def is_a?(arg)
			true
		end
	end
	class TestPersistable < Test::Unit::TestCase
		class ODBAExcluding
			include ODBA::Persistable
			ODBA_EXCLUDE_VARS = ["@excluded"]
			attr_accessor :included, :excluded
		end
		class ODBAContainer
		 include ODBA::Persistable
		 attr_accessor	:non_replaceable, :replaceable, :replaceable2, :array, :odba_persistent
		 attr_accessor	:odba_snapshot_level
		end
		class Hash
			include ODBA::Persistable
			attr_accessor :odba_persistent
		end
		class PersistableMock < Mock
			def is_a?(arg)
				true
			end
		end
		def setup
			ODBA.storage = Mock.new("storage")
			ODBA.marshaller = Mock.new("marshaller")
			@odba  = ODBAContainer.new
			ODBA.cache_server = Mock.new("cache_server")
		end
		def test_odba_id
			ODBA.storage.__next(:next_id) { ||
				2
			}
			ODBA.marshaller.__next(:dump) { |obj|
				"foo"
			}
			ODBA.storage.__next(:store) { |id,obj|}
			@odba.odba_take_snapshot(1)
			assert_equal(2, @odba.odba_id)
			ODBA.storage.__verify
			ODBA.marshaller.__verify
		end
		def test_odba_delete
			odba_container = ODBAContainer.new
			odba_container.odba_id = 2
			ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.cache_server.__next(:delete) { |object|
				assert_equal(odba_container, object)
			}
			odba_container.odba_delete
			ODBA.cache_server.__verify
		end
		def test_odba_replace_excluded
			odba = ODBAExcluding.new
			odba.included = "here to stay"
			odba.excluded = "easy come easy go"
			odba.odba_replace_excluded!
			assert_equal("here to stay", odba.included)
			assert_nil(odba.excluded)
		end
		def test_odba_replace_stubs
			stub = Mock.new
			@odba.replaceable = stub
			substitution = Mock.new
			@odba.odba_replace_stubs(stub, substitution)
			assert_equal(@odba.replaceable, substitution)
		end
		def test_odba_take_snapshot
			level1 = ODBAContainer.new
			level2 = ODBAContainer.new
			@odba.replaceable = level1
			level1.replaceable = level2

			ODBA.cache_server.__next(:store) { |obj| 2} 
			ODBA.cache_server.__next(:store) { |obj| 2}

			ODBA.cache_server.__next(:store) { |obj| 2}
			
			@odba.odba_take_snapshot
			assert_equal(1, @odba.odba_snapshot_level)
			assert_equal(1, level1.odba_snapshot_level)
			assert_equal(1, level2.odba_snapshot_level)
			ODBA.cache_server.__verify
		end
		def test_odba_unsaved_neighbors
			replaceable = PersistableMock.new
			@odba.replaceable = replaceable
=begin
			replaceable.__next(:is_a?) { |arg| false }
			replaceable.__next(:is_a?) { |arg| false }
=end
			replaceable.__next(:is_a?) { |arg| 
				assert_equal(Persistable, arg)
				true 
			}
			replaceable.__next(:odba_unsaved?){ |level| true}
			result = @odba.odba_unsaved_neighbors(2)
			assert_equal([replaceable], result)
			replaceable.__verify
		end
		def test_odba_unsaved_neighbors_2
			odba = ODBAExcluding.new
			included = PersistableMock.new
			excluded = PersistableMock.new
			odba.excluded  = excluded
			odba.included = included
=begin
			included.__next(:is_a?) { |klass|
				assert_equal(Hash, klass)
				false 
			}
			included.__next(:is_a?) { |klass|
				assert_equal(Array, klass)
				false 
			}
=end
			included.__next(:is_a?) { |klass|
				assert_equal(ODBA::Persistable, klass)
				true
			}
			included.__next(:odba_unsaved?) { true }
			result = odba.odba_unsaved_neighbors(2)
			assert_equal([included], result)
			excluded.__verify
			included.__verify
		end
		def test_odba_replaceable
			var = StubMock.new
			name = "foo"
			var.__next(:is_a?) { |persistable| true }
			assert_equal(true, @odba.odba_replaceable?(var, name))
			var.__verify
		end
		def test_extend_enumerable
			hash = Hash.new
			array = Array.new
			#@odba.odba_extend_enumerable(hash)
			#@odba.odba_extend_enumerable(array)
			assert_equal(true, hash.is_a?(Persistable))
			assert_equal(true, array.is_a?(Persistable))
		end
		def test_odba_replace_persistable
			replaceable = ODBAContainer.new
			non_rep = ODBAContainer.new
			non_rep.odba_id = 32
			replaceable.odba_id = 24
			@odba.replaceable = replaceable
			@odba.non_replaceable = non_rep
			@odba.odba_replace_persistable(replaceable)
			assert_equal(24, @odba.replaceable.odba_id)
			assert_equal(true, @odba.replaceable.is_a?(Stub))
			assert_equal(true, @odba.non_replaceable.is_a?(ODBAContainer))
		end
		def test_odba_replace_persistables
			replaceable = ODBAContainer.new
			replaceable.odba_id = 12
			non_replaceable = StubMock.new
			@odba.non_replaceable = non_replaceable
			@odba.replaceable = replaceable
			non_replaceable.__next(:is_a?) { |arg|
				assert_equal(Persistable, arg)
				false
			}
			ODBA.storage.__next(:next_id){||13 }
			@odba.odba_replace_persistables
			assert_instance_of(StubMock, @odba.non_replaceable)
			assert_equal(12, @odba.replaceable.odba_id)
			assert_equal(true, @odba.replaceable.is_a?(Stub))
			non_replaceable.__verify
			ODBA.cache_server.__verify
		end
		def test_odba_store_unsaved
			level1 = ODBAContainer.new
			level2 = ODBAContainer.new
			saved = ODBAContainer.new
			@odba.replaceable = level1
			@odba.non_replaceable = saved
			level1.replaceable = level2

			saved.odba_persistent = true
			ODBA.cache_server.__next(:store) { |obj| 2} 
			ODBA.cache_server.__next(:store)	{	|obj| 2}

			ODBA.cache_server.__next(:store)	{	|obj| 2}
			
			@odba.odba_store_unsaved
			assert_equal(nil, ODBA.cache_server.__verify)
		end
		def test_odba_store_unsaved_hash
			level1 = ODBAContainer.new
			hash_element = ODBAContainer.new
			hash = Hash.new
			non_rep_hash = Hash.new
			level1.replaceable = hash
			level1.non_replaceable = non_rep_hash
			non_rep_hash.odba_persistent = true
			
			ODBA.cache_server.__next(:store)	{	|obj| 2}
			ODBA.cache_server.__next(:store)	{	|obj| 2}
			
			level1.odba_store_unsaved
			assert_equal(nil, ODBA.cache_server.__verify)
		end
		def test_dup
			stub = StubMock.new("stub")
			stub2 = StubMock.new("stub2")
			@odba.replaceable = stub
			@odba.replaceable2 = stub2
			@odba.non_replaceable = 4
			stub.__next(:is_a?) { |arg| true}
			stub.__next(:odba_id) {|| 2}
			stub2.__next(:is_a?) { |arg| true}
			stub2.__next(:odba_id) { || 3}
			odba_twin = @odba.dup
			stub.__verify
			stub2.__verify
			assert_equal(2, odba_twin.replaceable.odba_id)
			assert_equal(3, odba_twin.replaceable2.odba_id)
			assert_equal(true, odba_twin.replaceable.is_a?(Stub))
			assert_equal(true, odba_twin.replaceable2.is_a?(Stub))
			assert_equal(stub, @odba.replaceable)
			assert_equal(stub2, @odba.replaceable2)
		end
=begin
		def test_odba_restore_persistables
			odba = ODBAContainer.new
			backup = {
				"@replaceable"	=>	@replaceable,
			}
			odba.odba_restore_persistables(backup)
			assert_equal(@replaceable, odba.replaceable)
		end
=end
		def test_odba_unsaved_true
			@odba.instance_variable_set("@odba_persistent", false)
			assert_equal(true, @odba.odba_unsaved?)
		end
		def test_odba_isolated_dump
			replaceable = StubMock.new("rep")
			replaceable2 = StubMock.new("rep2")
			@odba.replaceable2 = replaceable2
			@odba.replaceable = replaceable
			
			#only 4 expected calls because after the duplication
			# the calls will be made on ODBA::Stub objects
			replaceable.__next(:is_a?) { |arg| true}
			replaceable.__next(:odba_id) { 12 }
			replaceable2.__next(:is_a?) { |arg| true}
			replaceable2.__next(:odba_id) { 13 }
			ODBA.storage.__next(:next_id){|| 2}
			f_obj1 = Mock.new("fobj1")
			f_obj1.__next(:odba_carry_methods){[]}
			f_obj2 = Mock.new("fobj2")
			f_obj2.__next(:odba_carry_methods){[]}
			ODBA.cache_server.__next(:fetch) { |id,container| f_obj1}
			ODBA.cache_server.__next(:fetch) { |id,container| f_obj2}
			#ODBA.cache_server.__next(:add_object_connection){|id,id2|}
			#			ODBA.cache_server.__next(:add_object_connection){|id,id2|}
			
			ODBA.marshaller.__next(:dump) { |twin|
				assert_equal(true, twin.replaceable.is_a?(Stub))
				assert_equal(true, twin.replaceable2.is_a?(Stub))
				"TheDump"
			}
			result = @odba.odba_isolated_dump
			assert_equal(replaceable, @odba.replaceable)
			assert_equal("TheDump", result)
			assert(@odba.odba_target_ids.include?(12))
			assert(@odba.odba_target_ids.include?(13))
			#expected = [12, 13]
			#assert_equal(expected, @odba.odba_target_ids)
			replaceable.__verify
			ODBA.cache_server.__verify
		end
		def test_odba_isolated_dump_2
			ODBA.marshaller = ODBA::Marshal
			odba = ODBAExcluding.new
			odba.excluded = "foo"
			odba.included = "baz"
			ODBA.storage.__next(:next_id) { 1 }
			dump = odba.odba_isolated_dump
			obj = ODBA.marshaller.load(dump)
			assert_equal(nil, obj.excluded)
			assert_equal("baz", obj.included)
		end
		def test_odba_id
			@odba.odba_id = nil
			ODBA.storage.__next(:next_id) { 1 }
			assert_equal(1, @odba.odba_id)
			ODBA.storage.__verify
		end
		def test_odba_dump_has_id
			@odba.odba_id = nil
			ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.storage.__next(:next_id) { 1 }
			ODBA.marshaller = Marshal
			ODBA.cache_server.__next(:store) { |obj|
				assert_equal(1, obj.odba_id)
			}
			@odba.odba_store
			ODBA.storage.__verify
		end
		def test_odba_store_error_raised
			ODBA.marshaller = Marshal
			@odba.odba_name = "foo"
			cache_server = Mock.new
			ODBA.cache_server = cache_server
			ODBA.storage.__next(:transaction) { |block| block.call}
			cache_server.__next(:store) { |dump|
				raise DBI::ProgrammingError
			}
			assert_raises(DBI::ProgrammingError) {
				@odba.odba_store('baz')
			}
			assert_equal("foo", @odba.odba_name)
			cache_server.__verify
		end
		def test_odba_store_no_error_raised
			ODBA.marshaller = Marshal
			@odba.odba_name = "foo"
			cache_server = Mock.new
			ODBA.cache_server = cache_server
			ODBA.storage.__next(:transaction) { |block| block.call}
			cache_server.__next(:store) { |dump| }
			@odba.odba_store('bar')
			assert_equal("bar", @odba.odba_name)
			cache_server.__verify
		end
	end	
	class TestArrayReplaceStubs < Test::Unit::TestCase
		class StubMock < Mock
			def is_a?(arg)
				true
			end
		end
		class ODBAContainer
		 include ODBA::Persistable
		 attr_accessor	:non_replaceable, :replaceable, :array
		end
		def setup
			@array = Array.new
			ODBA.storage = Mock.new("storage")
			ODBA.marshaller = Mock.new("marshaller")
			#@array.extend(ODBA::PersistableArray)
			ODBA.cache_server = Mock.new("cache_server")
		end
		def test_odba_unsaved_neighbors_array
			rep1 = ODBAContainer.new
			rep2 = ODBAContainer.new
			@array.push(rep1)
			@array.push(rep2)
			result =@array.odba_unsaved_neighbors(1)
			assert_equal([rep1, rep2], result)
		end
		def test_array_replacement
			replacement = Mock.new
			replacement2 = Mock.new
			stub = StubMock.new
			stub2 = StubMock.new
			stub3 = StubMock.new
			foo = Mock.new("foo")
			stub.__next(:is_a?) { |arg| true}
			stub.__next(:odba_id) { 2 }
			stub.__next(:is_a?) { |arg| true}
			stub.__next(:odba_replace) { || }
			stub.__next(:receiver) { || replacement}
			stub2.__next(:is_a?) { |arg| true}
			stub2.__next(:odba_id) { 2 }
			stub2.__next(:is_a?) { |arg| true}
			stub2.__next(:odba_replace) { || }
			stub2.__next(:receiver) { || replacement2}
			stub3.__next(:is_a?) { |arg| false}
			stub3.__next(:is_a?) { |arg| false}
			ODBA.cache_server.__next(:bulk_fetch) { |ids, obj|}
			@array.push(stub)
			@array.push(stub2)
			@array.push(stub3)
			@array.odba_restore
			assert_equal(replacement, @array[0])
			assert_equal(replacement2, @array[1])
			assert_equal(stub3, @array[2])
			ODBA.cache_server.__verify
			stub.__verify
			stub2.__verify
			stub3.__verify
		end
=begin
		def test_odba_create_index
			ODBA.storage.__next(:create_index) { |index_name| 
				assert_equal("test_index", index_name)
			}
			ODBA.storage.__next(:store_index) { \
				|index_name, origin_id, search_term, target_id|
				assert_equal("test_index", index_name)
				assert_equal(12, origin_id)
				assert_equal("ponstan", search_term)
				assert_equal(2, target_id)
			}
			ODBA.create_index("test_index"){
				[ hash = {
					 "origin_id" => 12,
					 "search_term" => "ponstan",
					 "target_id"	=> 2,
					}
				]
			}
			ODBA.storage.__verify
		end
=end
		def test_odba_replace_persistables_array
			replaceable = StubMock.new("replaceable")
			replaceable2 = StubMock.new("replaceable2")
			@array.push(replaceable)
			@array.push(replaceable2)
			replaceable.__next(:is_a?) { |arg| true}
			replaceable.__next(:odba_id) { 1 }
			replaceable.__next(:odba_id) { 1 }
			
			replaceable2.__next(:is_a?) { |arg| true}
			replaceable2.__next(:odba_id) { 2 }
			replaceable2.__next(:odba_id) { 2 }

			ODBA.storage.__next(:next_id) { 1 }

			@array.odba_replace_persistables
			replaceable.__verify
			replaceable2.__verify
			ODBA.cache_server.__verify
			assert_equal(true, @array[0].is_a?(Stub))
			assert_equal(true, @array[1].is_a?(Stub))
			assert_equal(1, @array[0].odba_id)
			assert_equal(2, @array[1].odba_id)
		end
		def test_odba_unsaved_array_true
			val = StubMock.new("val")
			@array.instance_variable_set("@odba_persistent", true)
			@array.push(val)
			val.__next(:is_a?) { |klass| true }
			val.__next(:odba_unsaved?) { true }
			assert_equal(true, @array.odba_unsaved?)
		end
	end
end

