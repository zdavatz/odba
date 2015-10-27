# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'odba/version'

Gem::Specification.new do |spec|
  spec.name        = "odba"
  spec.version     = Odba::VERSION
  spec.author      = "Masaomi Hatakeyama, Zeno R.R. Davatz"
  spec.email       = "mhatakeyama@ywesee.com, zdavatz@ywesee.com"
  spec.description = "Object Database Access"
  spec.summary     = "Ruby Software for ODDB.org Memory Management"
  spec.homepage    = "https://github.com/zdavatz/odba"
  spec.license       = "GPL-v2"
  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
#  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'dbi'
  spec.add_dependency 'dbd-pg', '0.3.9'
  
  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "flexmock"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "minitest" if /^1\./.match(RUBY_VERSION)
end

