#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'
class Mock
	def odba_id
		1
	end
end
module ODBA
	module Persistable
		attr_accessor :odba_references
		attr_writer :odba_id
		public :odba_replace_excluded!
	end
	class TestPersistable < Test::Unit::TestCase
		class StubMock < Mock
			def is_a?(arg)
				true
			end
		end
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
			ODBA.cache_server = Mock.new("cache_server")
			#ODBA.scalar_cache = Mock.new("scalar_cache")
			@odba  = ODBAContainer.new
		end
		def teardown
			ODBA.storage.__verify
			ODBA.marshaller.__verify
			ODBA.cache_server.__verify
			#ODBA.scalar_cache.__verify
			ODBA.storage = nil
			ODBA.marshaller = nil
			ODBA.cache_server = nil
			#ODBA.scalar_cache = nil
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
			#ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.cache_server.__next(:delete) { |object|
				assert_equal(odba_container, object)
			}
			odba_container.odba_delete
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
			var.__next(:is_a?) { |stub| false }
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
			#ODBA.storage.__next(:next_id){ 13 }
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
			stub.__next(:is_a?) { |arg| true }
			stub_container = nil
			stub.__next(:odba_container=) { |obj| 
				stub_container = obj
			}
			stub2.__next(:is_a?) { |arg| true }
			stub_container2 = nil
			stub2.__next(:odba_container=) { |obj|
				stub_container2 = obj
			}
			odba_twin = @odba.dup
			odba_twin.replaceable.__verify
			odba_twin.replaceable2.__verify
			assert_equal(odba_twin, stub_container)	
			assert_equal(odba_twin, stub_container2)	
		end
		def test_odba_unsaved_true
			@odba.instance_variable_set("@odba_persistent", false)
			assert_equal(true, @odba.odba_unsaved?)
		end
		def test_odba_target_ids
			replaceable = StubMock.new("rep")
			replaceable2 = StubMock.new("rep2")
			@odba.replaceable = replaceable
			@odba.replaceable2 = replaceable2
			replaceable.__next(:is_a?) { |arg| 
				true # is_a?(Persistable) 
			}
			replaceable.__next(:odba_id) { 12 }
			replaceable2.__next(:is_a?) { |arg| false }
			expected = [12]
			assert_equal(expected, @odba.odba_target_ids.sort)
			replaceable.__verify
			replaceable2.__verify
		end
		def test_odba_isolated_dump
			replaceable = StubMock.new("rep")
			replaceable2 = StubMock.new("rep2")
			@odba.replaceable = replaceable
			@odba.replaceable2 = replaceable2
			ODBA.storage.__next(:next_id){ 11 }

			replaceable2.__next(:is_a?){false}
			replaceable2.__next(:is_a?){true}
			replaceable2.__next(:is_a?){false}
			replaceable2.__next(:odba_id){ 12}

			replaceable.__next(:is_a?){false}
			replaceable.__next(:is_a?){true}
			replaceable.__next(:is_a?){true}
			ODBA.marshaller.__next(:dump) { |twin|
				"TheDump"
			}
			result = @odba.odba_isolated_dump
			assert_equal(replaceable, @odba.replaceable)
			assert_equal(replaceable2, @odba.replaceable2)
			assert_equal("TheDump", result)
			replaceable.__verify
			replaceable2.__verify
		end
		def test_odba_isolated_dump_2
			tmp = ODBA.marshaller
			ODBA.marshaller = ODBA::Marshal
			odba = ODBAExcluding.new
			odba.excluded = "foo"
			odba.included = "baz"
			ODBA.storage.__next(:next_id) { 1 }
			dump = odba.odba_isolated_dump
			obj = ODBA.marshaller.load(dump)
			assert_equal(nil, obj.excluded)
			assert_equal("baz", obj.included)
			ODBA.marshaller = tmp
		end
		def test_odba_id
			@odba.odba_id = nil
			ODBA.storage.__next(:next_id) { 1 }
			assert_equal(1, @odba.odba_id)
			ODBA.storage.__verify
		end
		def test_odba_dump_has_id
			@odba.odba_id = nil
			#ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.storage.__next(:next_id) { 1 }
			ODBA.cache_server.__next(:store) { |obj|
				assert_equal(1, obj.odba_id)
			}
			@odba.odba_store
		end
		def test_odba_store_error_raised
			@odba.odba_name = "foo"
			#ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.cache_server.__next(:store) { |dump|
				raise DBI::ProgrammingError
			}
			assert_raises(DBI::ProgrammingError) {
				@odba.odba_store('baz')
			}
			assert_equal("foo", @odba.odba_name)
		end
		def test_odba_store_no_error_raised
			@odba.odba_name = "foo"
			#ODBA.storage.__next(:transaction) { |block| block.call}
			ODBA.cache_server.__next(:store) { |obj| 
				assert_equal(@odba, obj)
			}
			@odba.odba_store('bar')
			assert_equal("bar", @odba.odba_name)
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
			ODBA.cache_server = Mock.new("cache_server")
		end
		def test_odba_cut_connection
			remove_obj = Mock.new("receiver")
			remove_obj.extend(ODBA::Persistable)
			remove_obj.odba_id = 2
			receiver = ODBA::Stub.new(2,self, remove_obj)
			array = Array.new
			array.push(receiver)
			assert_equal(0, array.odba_cut_connection(remove_obj).size)
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
			replacement = Mock.new('replacement')
			replacement2 = Mock.new('replacement2')
			stub = StubMock.new('stub')
			stub2 = StubMock.new('stub2')
			stub3 = StubMock.new('stub3')
			stub3.__next(:is_a?) { false }
			foo = Mock.new("foo")
			@array.push(stub)
			@array.push(stub2)
			@array.push(stub3)
			@array.odba_restore([[0,replacement], [1,replacement2]])
			assert_equal(replacement, @array[0])
			assert_equal(replacement2, @array[1])
			assert_equal(stub3, @array[2])
			ODBA.cache_server.__verify
			stub.__verify
			stub2.__verify
			stub3.__verify
		end
		def test_odba_replace_persistables_array
			replaceable = StubMock.new("replaceable")
			replaceable2 = StubMock.new("replaceable2")
			@array.push(replaceable)
			@array.push(replaceable2)
			#replaceable.__next(:is_a?) { |arg| true}
			#replaceable.__next(:odba_id) { 1 }
			#replaceable.__next(:odba_id) { 1 }
			
			#replaceable2.__next(:is_a?) { |arg| true}
			#replaceable2.__next(:odba_id) { 2 }
			#replaceable2.__next(:odba_id) { 2 }

			ODBA.storage.__next(:next_id) { 1 }

			@array.odba_replace_persistables
			replaceable.__verify
			replaceable2.__verify
			ODBA.cache_server.__verify
			#size is 0 because we store empty array in the db
			# content of the array is in the collection table
			assert_equal(0, @array.size)
			#assert_equal(true, @array[0].is_a?(Stub))
			#assert_equal(true, @array[1].is_a?(Stub))
			#assert_equal(1, @array[0].odba_id)
			#assert_equal(2, @array[1].odba_id)
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
