require 'bundler/setup'
require 'rspec/core/rake_task'
require 'yard'

RSpec::Core::RakeTask.new(:spec)

YARD::Rake::YardocTask.new

desc 'Default: run specs.'
task :default => :spec
