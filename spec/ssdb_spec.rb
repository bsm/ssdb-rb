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
    res["leveldb.stats"].keys.should =~ ["compactions", "level", "read", "size", "time", "written"]
  end

  describe "scripting" do

    it 'should eval' do
      subject.eval("return 'hello'").should == "hello"
      subject.eval("return table.concat(args)", 1, "a", 2, "b").should == "1a2b"
      subject.eval("local x = 10 * math.pi; return x").should == "31.415927"
      subject.eval("return 2 ^ 3 == 8").should == "1"
      subject.eval("local no_return").should be_nil
      subject.eval("return {1, 'a', 2, 'b', ['c'] = 3, false, function() end, { x = 5 }, 'd'}").should == ["1", "a", "2", "b", "0", "d"]
      subject.eval("return 1, 2").should == "1"
    end

    it 'should raise on eval failures' do
      -> { subject.eval "wrong syntax" }.should raise_error(SSDB::CommandError, /failed compiling/)
    end

    it 'should expose ssdb instance' do
      subject.eval("return type(ssdb)").should == "userdata"
      subject.eval("return type(getmetatable(ssdb))").should == "table"
      subject.eval("return type(ssdb)").should == "userdata"
      subject.eval("return type(getmetatable(ssdb))").should == "table"
    end

    it 'should expose ssdb methods' do
      subject.set("key", "v1")
      subject.eval("return ssdb:get('key')").should == "v1"
      subject.eval("return ssdb:get('missing')").should be_nil

      -> { subject.eval "return ssdb:no_method()" }.should raise_error(SSDB::CommandError, /failed running/)
    end

    it 'should increment' do
      subject.eval(%(
        local res = { ssdb:incr('key') }
        for i=2,5 do res[i] = ssdb:incr('key', i) end
        return res
      )).should == ["1", "3", "6", "10", "15"]
    end

  end
end