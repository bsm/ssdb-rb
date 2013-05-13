require 'bundler/setup'
require 'rspec'
require 'ssdb'

# Fixture prefix
FPX = "ssdb:rb:spec"

RSpec.configure do |c|

  c.after do
    db = SSDB.current

    # Remove keys
    db.keys(FPX, FPX + "\xFF").each do |key|
      db.del(key)
    end

    # Remove zsets
    db.zlist(FPX, FPX + "\xFF").each do |key|
      db.multi_zdel key, db.zkeys(key, 0, 1_000_000)
    end
  end

end
