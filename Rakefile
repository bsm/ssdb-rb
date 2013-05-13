require 'bundler/setup'
require 'rspec/core/rake_task'
require 'yard'

RSpec::Core::RakeTask.new(:spec)

RSpec::Core::RakeTask.new(:coverage) do |c|
  c.ruby_opts = '-r ./spec/coverage_helper'
end

YARD::Rake::YardocTask.new

desc 'Default: run specs.'
task :default => :spec
