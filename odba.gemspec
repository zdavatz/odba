require "rubygems"
require "rake"

spec = Gem::Specification.new do |s|
   s.name        = "odba"
   s.version     = "1.9"
   s.summary     = "Object Database Access used for a lot of ywesee products."
   s.description = "used in bbmb, oddb.org, and any ywesee product that uses Postgresql."
   s.author      = "Hannes Wyss, Masaomi Hatakeyama"
   s.email       = "hwyss@ywesee.com, mhatakeyama@ywesee.com"
   s.platform    = Gem::Platform::RUBY
   s.files       = FileList['lib/*.rb', 'bin/*', '[A-Z]*', 'test/*', 
                            'test/data/*.xls'].to_a
   s.homepage	 = "http://scm.ywesee.com/odba/.git"
end

if $0 == __FILE__
   Gem.manage_gems
   Gem::Builder.new(spec).build
end
