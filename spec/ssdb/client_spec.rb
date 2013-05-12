require 'spec_helper'

describe SSDB::Client do

  its(:url)       { should be_instance_of(URI::Generic) }
  its(:timeout)   { should == 10.0 }
  its(:id)        { should == "ssdb://127.0.0.1:8888/" }
  its(:port)      { should == 8888 }
  its(:reconnect) { should be(true) }

  it "should not be connected by default" do
    subject.should_not be_connected
  end

  it "can connect" do
    -> { subject.send(:socket) }.should change { subject.connected? }.to(true)
  end

  it "can disconnect" do
    -> { subject.disconnect }.should_not change { subject.connected? }
    subject.send(:socket)
    -> { subject.disconnect }.should change { subject.connected? }.to(false)
  end

  it "should perform commands" do
    res = subject.perform [{ cmd: ["set", "#{FPX}:key", "VAL1"] }]
    res.should == ["1"]
  end

  it "should perform commands in bulks" do
    res = subject.perform [
      { cmd: ["set", "#{FPX}:key", "VAL2"] },
      { cmd: ["get", "#{FPX}:key"] }
    ]
    res.should == ["1", "VAL2"]
  end

  it "should perform complex command chains" do
    res = subject.perform [
      { cmd: ["zset", "#{FPX}:zset", "VAL1", "1"] },
      { cmd: ["zset", "#{FPX}:zset", "VAL2", "2"] },
      { cmd: ["zscan", "#{FPX}:zset", "*", "0", "5", "-1"], multi: true },
      { cmd: ["zrscan", "#{FPX}:zset", "*", "5", "0", "-1"], multi: true, proc: ->r { r[1] = r[1].to_f; r } }
    ]
    res.should == ["1", "1", ["VAL1", "1", "VAL2", "2"], ["VAL2", 2, "VAL1", "1"]]
  end

end