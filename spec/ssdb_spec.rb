require 'spec_helper'

describe SSDB do

  describe "class" do
    subject { described_class }

    its(:current) { should be_instance_of(described_class) }
    it { should respond_to(:current=) }
  end

  it 'should execute batches' do
    res = subject.batch do
      subject.set  "#{FPX}:key", "100"
      subject.get  "#{FPX}:key"
      subject.incr "#{FPX}:key", 10
      subject.decr "#{FPX}:key", 30
    end
    res.should == [true, "100", 110, 80]
  end

  it 'should execute batches with futures' do
    s = n = nil
    subject.batch do
      subject.set  "#{FPX}:key", "100"
      s = subject.get  "#{FPX}:key"
      n = subject.incr "#{FPX}:key", 10

      -> { s.value }.should raise_error(SSDB::FutureNotReady)
    end

    s.should be_instance_of(SSDB::Future)
    s.value.should == "100"
    n.value.should == 110
  end

  it 'should retrieve info' do
    res = subject.info
    res.should be_instance_of(Hash)
    res.should include("version")
    res.should include("cmd.get")
    res.should include("leveldb.stats")

    res["cmd.get"].should be_instance_of(Hash)
    res["cmd.get"].keys.should =~ ["calls", "time_wait", "time_proc"]

    res["leveldb.stats"].should be_instance_of(Hash)
    res["leveldb.stats"].should have(6).keys
  end

  it 'should eval scripts' do
    subject.eval("return 'hello'").should == "hello"
    subject.eval("return table.concat(args)", 1, "a", 2, "b").should == "1a2b"
    subject.eval("local x = 10 * math.pi; return x").should == "31.415927"
    subject.eval("return 2 ^ 3 == 8").should == "1"
    subject.eval("local no_return").should be_nil
    # TODO
    # subject.eval("return {1, 'a', 2, 'b', ['c'] = 3}").should == ["1", "a", "2", "b", "c", "3"]

    -> { subject.eval "wrong syntax" }.should raise_error(SSDB::CommandError, /failed compiling/)
  end

end