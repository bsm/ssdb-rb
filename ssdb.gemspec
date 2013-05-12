# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "ssdb/version"

Gem::Specification.new do |s|
  s.name        = "ssdb"
  s.version     = SSDB::VERSION.dup
  s.platform    = Gem::Platform::RUBY
  s.licenses    = ["MIT"]
  s.summary     = "A Ruby client library for SSDB"
  s.email       = "info@blacksquaremedia.com"
  s.homepage    = "http://github.com/bsm/ssdb-rb"
  s.description = "Please see https://github.com/ideawu/ssdb/ for more information."
  s.authors     = ['Dimitrij Denissenko']

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ["lib"]

  s.add_development_dependency("rspec")
  s.add_development_dependency("rake")
  s.add_development_dependency("bundler")
end
