#!/usr/bin/env ruby
#-- ODBA -- odba -- 13.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com
#	Copyright (C) 2004 Hannes Wyss
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#	ywesee - intellectual capital connected, Winterthurerstrasse 52, CH-8006 Zürich, Switzerland
#	hwyss@ywesee.com
#++
# = ODBA - Object DataBase Access
#
# ODBA is an unintrusive Object Cache system. It adresses the crosscutting 
# concern of object storage by disconnecting and serializing objects into 
# storage. All disconnected connections are replaced by instances of 
# ODBA::Stub, thus enabling transparent object-loading.
# 
# ODBA supports: 
# * transparent loading of connected objects
# * index-vectors
# * transactions
# * transparently fetches Hash-Elements without loading the entire Hash
#
# == Example
#		include 'odba'
#
#		# connect default storage manager to a relational database 
#		ODBA.storage.dbi = ODBA::ConnectionPool.new('DBI::pg::database', 'user', 'pw')
# 
#		class Counter 
#			include ODBA::Persistable
#			def initialize
#				@pos = 0
#			end
#			def up
#				@pos += 1
#				self.odba_store
#				@pos
#			end
#			def down
#				@pos -= 1
#				self.odba_store
#				@pos
#			end
#		end
#
# :main:lib/odba.rb

puts __FILE__
require 'odba/persistable'
require 'odba/storage'
require 'odba/cache'
require 'odba/stub'
require 'odba/marshal'
require 'odba/cache_entry'
require 'odba/odba_error'
require 'odba/index'
require 'odba/odba'

