require 'spec_helper'

describe SSDB::Client do

  def perform(*cmds)
    subject.perform cmds.map {|c| { cmd: c } }
  end

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
    perform(
      ["set", "#{FPX}:key", "VAL1"]
    ).should == ["1"]
  end

  it "should perform commands in bulks" do
    perform(
      ["set", "#{FPX}:key", "VAL2"],
      ["get", "#{FPX}:key"],
    ).should == ["1", "VAL2"]
  end

  it "should parse responses" do
    perform(
      ["set", "#{FPX}:key", "\nabcd"],
      ["get", "#{FPX}:key"],
      ["set", "#{FPX}:key", "ab\ncd"],
      ["get", "#{FPX}:key"],
      ["set", "#{FPX}:key", "abcd\n"],
      ["get", "#{FPX}:key"]
    ).should == ["1", "\nabcd", "1", "ab\ncd", "1", "abcd\n"]
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

  it "should handle missing results" do
    perform(
      ["get", "#{FPX}:key"],
      ["set", "#{FPX}:key", "a"],
      ["get", "#{FPX}:key"]
    ).should == [nil, "1", "a"]
  end

  it "should handle client errors" do
    perform(["set", "#{FPX}:key", "a"]).should == ["1"]
    -> { perform(["invalid", "command"]) }.should raise_error(SSDB::CommandError, /client_error/)
    perform(["get", "#{FPX}:key"]).should == ["a"]
  end

  it "should retry on connection errors" do
    perform(["set", "#{FPX}:key", "a"]).should == ["1"]

    sock = subject.send(:socket)
    sock.should_receive(:gets).and_raise(Errno::EPIPE)

    perform(["get", "#{FPX}:key"]).should == ["a"]
    subject.send(:socket).should_not be(sock)
  end

end